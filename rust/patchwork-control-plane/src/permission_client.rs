use anyhow::{Result, bail};

use crate::auth::{Principal, PrincipalType};

pub enum Action {
    CreateTable,
    DeleteTable,
    Read,
    Write,
    ReadTelemetry,
    ResetTelemetry,
    #[cfg(feature = "test-api")]
    SplitPartition,
    #[cfg(feature = "test-api")]
    IncreaseReplication,
}

pub enum Resource {
    Account(i64),
    Table(i64),
    Cluster,
}

#[derive(Clone, Default)]
pub struct PermissionClient;

impl PermissionClient {
    pub async fn check(
        &self,
        principal: &Principal,
        _action: Action,
        _resource: Resource,
    ) -> Result<()> {
        if principal.principal_type == PrincipalType::AtgsInternalService {
            return Ok(());
        }
        // Replace with the permission service call once that service exists.
        bail!("Permission service is not configured")
    }
}
