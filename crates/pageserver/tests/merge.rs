/// Page delta merge tests — exercises `TimelineManager::merge_branch`.
///
/// Claim: branch changes can be merged forward into any target timeline.
/// The merge is O(pages_changed), not O(total_pages).  Source wins on conflict.
///
/// Scenario:
///   root (5 pages at LSN 1000)
///     └── feature-branch (4 pages at LSN 2000: 3 new + 1 conflicting)
///
/// After merge at LSN 3000:
///   - root gains the 3 new pages from the branch
///   - root's conflicting page is overwritten by the branch version (source wins)
///   - the branch itself is unchanged
///   - a sibling branch sees neither root's post-branch writes nor the merged pages

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use lattice_common::{Lsn, TenantId, RelTag, PageImage, PAGE_SIZE};
use lattice_pageserver::{
    redo::RedoEngine,
    store::MemoryLayerStorage,
    timeline::TimelineManager,
};

fn rel(n: u32) -> RelTag { RelTag::main(0, 1, n) }

fn page(b: u8) -> PageImage {
    PageImage::new(Bytes::from(vec![b; PAGE_SIZE]))
}

fn setup() -> (Arc<TimelineManager>, RedoEngine) {
    let storage = Arc::new(MemoryLayerStorage::new());
    (Arc::new(TimelineManager::new(storage)), RedoEngine::new())
}

// ── Test 1: basic merge with conflict resolution ─────────────────────────────

#[test]
fn merge_branch_into_parent() {
    let (mgr, redo) = setup();
    let tenant = TenantId::new();

    // Root timeline: 5 pages at LSN 1000
    let root = mgr.create_timeline(tenant, "root".into());
    for i in 0..5u32 {
        root.put_image(rel(i), 0, Lsn(1000), page(0xAA));
    }

    // Branch from root at LSN 1000
    let (branch, _) = mgr.create_branch(root.id(), tenant, Lsn(1000), "feature".into()).unwrap();

    // Feature branch writes:
    //   rel(10), rel(11), rel(12) — new pages not on root
    //   rel(0)  — conflicts with root's rel(0)
    branch.put_image(rel(10), 0, Lsn(2000), page(0xBB));
    branch.put_image(rel(11), 0, Lsn(2000), page(0xCC));
    branch.put_image(rel(12), 0, Lsn(2000), page(0xDD));
    branch.put_image(rel(0),  0, Lsn(2000), page(0xEE)); // conflict

    // Root also modifies rel(0) after the branch point
    root.put_image(rel(0), 0, Lsn(1500), page(0xFF));

    let t0 = Instant::now();
    let result = mgr.merge_branch(tenant, branch.id(), root.id(), Lsn(3000)).unwrap();
    let wall = t0.elapsed();

    // ── assertions ───────────────────────────────────────────────────────────

    // Merged pages are visible on root at merge_lsn
    let p = root.get_page_at_lsn(&redo, rel(10), 0, Lsn(3000)).unwrap();
    assert_eq!(p.0[0], 0xBB, "rel(10) wrong after merge");

    let p = root.get_page_at_lsn(&redo, rel(11), 0, Lsn(3000)).unwrap();
    assert_eq!(p.0[0], 0xCC, "rel(11) wrong after merge");

    let p = root.get_page_at_lsn(&redo, rel(12), 0, Lsn(3000)).unwrap();
    assert_eq!(p.0[0], 0xDD, "rel(12) wrong after merge");

    // Source wins on conflict: branch's 0xEE beats root's 0xFF
    let p = root.get_page_at_lsn(&redo, rel(0), 0, Lsn(3000)).unwrap();
    assert_eq!(p.0[0], 0xEE, "source should win on conflict");

    // Branch itself is unchanged
    let p = branch.get_page_at_lsn(&redo, rel(10), 0, Lsn(2000)).unwrap();
    assert_eq!(p.0[0], 0xBB, "source branch should be unchanged by merge");

    assert_eq!(result.merged_pages, 4, "4 pages merged (3 new + 1 conflict)");
    assert_eq!(result.conflicts, 1, "1 conflict: rel(0) modified on both sides");
    assert!(result.elapsed_us < 10_000, "merge took too long: {} μs", result.elapsed_us);

    println!("\n[PAGE DELTA MERGE]");
    println!("  root pages:     5   (LSN 1000)");
    println!("  branch pages:   4   (3 new + 1 conflicting, LSN 2000)");
    println!("  merge strategy: source-wins");
    println!("  merged pages:   {}", result.merged_pages);
    println!("  conflicts:      {} (rel=0 modified on both sides)", result.conflicts);
    println!("  merge time:     {} μs", result.elapsed_us);
    println!("  total elapsed:  {:?}", wall);
    println!("  throughput:     {:.1} pages/μs",
        result.merged_pages as f64 / result.elapsed_us.max(1) as f64);
}

// ── Test 2: merge does not affect sibling branches ──────────────────────────

#[test]
fn merge_does_not_affect_siblings() {
    let (mgr, redo) = setup();
    let tenant = TenantId::new();

    let root = mgr.create_timeline(tenant, "root".into());
    root.put_image(rel(1), 0, Lsn(1000), page(0x01));

    let (branch_a, _) = mgr.create_branch(root.id(), tenant, Lsn(1000), "a".into()).unwrap();
    let (branch_b, _) = mgr.create_branch(root.id(), tenant, Lsn(1000), "b".into()).unwrap();

    // Only branch_a modifies rel(1)
    branch_a.put_image(rel(1), 0, Lsn(2000), page(0xAA));

    // Merge branch_a into root
    let result = mgr.merge_branch(tenant, branch_a.id(), root.id(), Lsn(3000)).unwrap();
    assert_eq!(result.merged_pages, 1);

    // branch_b must still see the original value (0x01), not the merged one
    let p = branch_b.get_page_at_lsn(&redo, rel(1), 0, Lsn(1000)).unwrap();
    assert_eq!(p.0[0], 0x01, "branch_b should not see branch_a's merged pages");

    println!("\n[MERGE ISOLATION] sibling branch unaffected by merge ✓");
}

// ── Test 3: merge throughput — large divergent set ──────────────────────────

#[test]
fn merge_throughput() {
    const N: usize = 1000; // pages diverged on the branch

    let (mgr, _redo) = setup();
    let tenant = TenantId::new();

    let root = mgr.create_timeline(tenant, "root".into());
    root.put_image(rel(0), 0, Lsn(1000), page(0x00)); // at least one page so branch_lsn is meaningful

    let (branch, _) = mgr.create_branch(root.id(), tenant, Lsn(1000), "big-feature".into()).unwrap();

    for i in 0..N as u32 {
        branch.put_image(rel(100 + i), 0, Lsn(2000), page(0xBB));
    }

    let t0 = Instant::now();
    let result = mgr.merge_branch(tenant, branch.id(), root.id(), Lsn(3000)).unwrap();
    let wall = t0.elapsed();

    assert_eq!(result.merged_pages, N);
    assert_eq!(result.conflicts, 0);

    let throughput = N as f64 / wall.as_secs_f64();
    println!("\n[MERGE THROUGHPUT]");
    println!("  pages merged:   {N}");
    println!("  elapsed:        {} μs", result.elapsed_us);
    println!("  throughput:     {:.0} pages/sec ({:.1} μs/page)",
        throughput, result.elapsed_us as f64 / N as f64);
}
