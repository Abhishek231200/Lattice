use std::fmt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! typed_uuid {
    ($name:ident, $display_prefix:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            pub fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            pub fn as_uuid(&self) -> &Uuid {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}{}", $display_prefix, self.0)
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let s = s.strip_prefix($display_prefix).unwrap_or(s);
                Ok(Self(Uuid::parse_str(s)?))
            }
        }
    };
}

typed_uuid!(TenantId, "t-");
typed_uuid!(TimelineId, "tl-");
