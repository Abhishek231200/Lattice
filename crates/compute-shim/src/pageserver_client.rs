/// HTTP client for the pageserver's get_page_at_lsn endpoint.
/// Used by both the shim service and the C extension (via FFI).

use anyhow::Result;
use bytes::Bytes;
use reqwest::Client;

use lattice_common::{Lsn, TenantId, TimelineId, RelTag, BlockNumber, PageImage, PAGE_SIZE};
use lattice_common::proto::{GetPageRequest, GetPageResponse};

pub struct PageserverClient {
    base_url: String,
    client: Client,
    tenant_id: TenantId,
    timeline_id: TimelineId,
}

impl PageserverClient {
    pub fn new(
        base_url: impl Into<String>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap(),
            tenant_id,
            timeline_id,
        }
    }

    /// Fetch a page from the pageserver.
    pub async fn get_page(
        &self,
        rel: RelTag,
        blk: BlockNumber,
        lsn: Lsn,
    ) -> Result<PageImage> {
        let req = GetPageRequest {
            tenant_id: self.tenant_id,
            timeline_id: self.timeline_id,
            rel,
            blk,
            lsn,
        };

        let resp: GetPageResponse = self.client
            .post(format!("{}/page", self.base_url))
            .json(&req)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        if resp.page.len() != PAGE_SIZE {
            anyhow::bail!(
                "pageserver returned wrong page size: {} (expected {})",
                resp.page.len(), PAGE_SIZE
            );
        }

        Ok(PageImage::new(Bytes::from(resp.page)))
    }

    /// Push a page to the pageserver (used by tests and the WAL redo path).
    pub async fn put_page(
        &self,
        rel: RelTag,
        blk: BlockNumber,
        lsn: Lsn,
        page: &PageImage,
    ) -> Result<()> {
        use lattice_common::proto::PutPageRequest;
        let req = PutPageRequest {
            tenant_id: self.tenant_id,
            timeline_id: self.timeline_id,
            rel,
            blk,
            lsn,
            page: page.as_bytes().to_vec(),
        };
        self.client
            .post(format!("{}/page/put", self.base_url))
            .json(&req)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}
