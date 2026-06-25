/// Storage amplification test — measures the "1/4 footprint" COW claim.
///
/// Claim: a branch costs ~0 bytes at creation time, and subsequent writes to
/// the branch cost exactly the bytes written (not a copy of the entire parent).
///
/// Protocol:
///   1. Write N full pages to the root timeline  → baseline = N × PAGE_SIZE
///   2. Create branch  (O(1) — one pointer, no pages copied)
///   3. Read a page on the branch → returns parent's page (COW reads through)
///   4. Write M pages on the branch (M << N)
///   5. Compare: root = N pages, branch = M pages (not N pages)
///
/// The footprint of the branch relative to the parent is M/N.
/// With M=10, N=1000 that is 0.8 KB / 8 MB = 1% of the parent's size.

use std::sync::Arc;

use bytes::Bytes;
use lattice_common::{Lsn, TenantId, RelTag, PageImage, PAGE_SIZE};
use lattice_pageserver::{
    redo::RedoEngine,
    store::MemoryLayerStorage,
    timeline::TimelineManager,
};

fn rel(relnode: u32) -> RelTag {
    RelTag::main(0, 1, relnode)
}

fn page(byte: u8) -> PageImage {
    PageImage::new(Bytes::from(vec![byte; PAGE_SIZE]))
}

fn setup() -> (Arc<TimelineManager>, Arc<RedoEngine>) {
    let storage = Arc::new(MemoryLayerStorage::new());
    let mgr = Arc::new(TimelineManager::new(storage));
    let redo = Arc::new(RedoEngine::new());
    (mgr, redo)
}

#[test]
fn storage_amplification_cow_branching() {
    let (mgr, redo) = setup();
    let tenant = TenantId::new();

    const N_PARENT_PAGES: usize = 1000; // 8 MB of pages
    const M_BRANCH_PAGES: usize = 10;   // pages written to the branch

    // ── Step 1: Write N pages to root timeline ────────────────────────────────
    let root = mgr.create_timeline(tenant, "root".into());
    for i in 0..N_PARENT_PAGES {
        root.put_image(rel(i as u32), 0, Lsn(1000), page(0xAA));
    }

    let root_stats = root.layer_stats();
    let root_bytes = root_stats.logical_bytes();
    println!("\n[STORAGE AMPLIFICATION]");
    println!("  Root timeline: {N_PARENT_PAGES} pages written");
    println!("    image layers:  {}", root_stats.image_layers);
    println!("    total pages:   {}", root_stats.total_image_pages);
    println!("    logical size:  {:.1} MB", root_stats.logical_mb());

    assert_eq!(root_stats.total_image_pages, N_PARENT_PAGES);

    // ── Step 2: Create branch (O(1) — no pages copied) ───────────────────────
    let (branch, elapsed) = mgr.create_branch(root.id(), tenant, Lsn(1000), "branch".into())
        .expect("create_branch failed");

    let branch_stats_empty = branch.layer_stats();
    println!("\n  Branch created in {} μs (O(1) — zero pages copied)", elapsed.as_micros());
    println!("    branch image layers: {} (empty — inherits via pointer)", branch_stats_empty.image_layers);
    println!("    branch total pages:  {}", branch_stats_empty.total_image_pages);
    println!("    branch logical size: {:.1} KB", branch_stats_empty.logical_kb());

    // Branch starts with zero image layers — all pages inherited via parent pointer
    assert_eq!(branch_stats_empty.total_image_pages, 0,
        "branch should have 0 own pages at creation");

    // ── Step 3: Verify COW reads through to parent ───────────────────────────
    let page_on_branch = branch.get_page_at_lsn(&redo, rel(0), 0, Lsn(1000))
        .expect("get_page_at_lsn on branch should read through to parent");
    assert_eq!(page_on_branch.0[0], 0xAA,
        "branch returned wrong page data (expected parent's 0xAA byte)");
    println!("\n  COW read-through: branch correctly reads parent's page at LSN 1000");

    // ── Step 4: Write M pages to branch ─────────────────────────────────────
    for i in 0..M_BRANCH_PAGES {
        // Write to different relations than the parent to simulate new writes
        branch.put_image(rel((N_PARENT_PAGES + i) as u32), 0, Lsn(2000), page(0xBB));
    }

    let branch_stats = branch.layer_stats();
    let branch_bytes = branch_stats.logical_bytes();
    let amplification_pct = branch_bytes as f64 / root_bytes as f64 * 100.0;

    println!("\n  After writing {M_BRANCH_PAGES} pages to branch:");
    println!("    branch image layers: {}", branch_stats.image_layers);
    println!("    branch own pages:    {}", branch_stats.total_image_pages);
    println!("    branch logical size: {:.1} KB", branch_stats.logical_kb());
    println!("\n  ROOT  logical size: {:.2} MB ({N_PARENT_PAGES} pages)", root_stats.logical_mb());
    println!("  BRANCH logical size: {:.2} KB ({M_BRANCH_PAGES} pages)", branch_stats.logical_kb());
    println!("  Amplification ratio: {amplification_pct:.1}% of parent size");
    println!("  (vs 100% for a full copy — {}x more efficient)",
        (root_bytes as f64 / branch_bytes.max(1) as f64) as usize);

    // Branch must NOT have copied parent's pages
    assert_eq!(branch_stats.total_image_pages, M_BRANCH_PAGES,
        "branch should have exactly M_BRANCH_PAGES own pages, not N_PARENT_PAGES");

    // Amplification must be much less than 100%
    assert!(amplification_pct < 10.0,
        "amplification too high: {amplification_pct:.1}% — COW is not working");

    // Parent must be unchanged
    let root_stats_after = root.layer_stats();
    assert_eq!(root_stats_after.total_image_pages, N_PARENT_PAGES,
        "root timeline pages changed after writing to branch — COW isolation broken");
}

#[test]
fn branch_write_isolation() {
    // Writes on parent after branch point must NOT appear on branch.
    let (mgr, redo) = setup();
    let tenant = TenantId::new();

    let root = mgr.create_timeline(tenant, "root".into());
    root.put_image(rel(1), 0, Lsn(100), page(0x11));

    let (branch, _) = mgr.create_branch(root.id(), tenant, Lsn(100), "branch".into()).unwrap();

    // Write on root AFTER branch point
    root.put_image(rel(1), 0, Lsn(200), page(0x22));

    // Branch should still see 0x11 (from before the branch point)
    let p = branch.get_page_at_lsn(&redo, rel(1), 0, Lsn(100)).unwrap();
    assert_eq!(p.0[0], 0x11, "branch incorrectly saw post-branch parent write");

    // Root should see 0x22
    let p = root.get_page_at_lsn(&redo, rel(1), 0, Lsn(200)).unwrap();
    assert_eq!(p.0[0], 0x22);

    println!("\n[WRITE ISOLATION] Parent post-branch writes are invisible to branch ✓");
}
