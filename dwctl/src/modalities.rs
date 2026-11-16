
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Modality {
    Realtime,
    Batch,
}

impl Modality {
    pub const ALL: [Modality; 2] = [Modality::Realtime, Modality::Batch];

    pub fn as_str(self) -> &'static str {
        match self {
            Modality::Realtime => "realtime",
            Modality::Batch => "batch",
        }
    }

    pub fn disabled_message(self) -> String {
        let surface = match self {
            Modality::Realtime => "Realtime inference",
            Modality::Batch => "The batch API",
        };
        format!("{surface} is disabled for this organization. An organization owner can re-enable it in the organization settings.")
    }

    fn bit(self) -> u8 {
        match self {
            Modality::Realtime => 0b01,
            Modality::Batch => 0b10,
        }
    }
}

impl fmt::Display for Modality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown modality '{0}'; expected one of: realtime, batch")]
pub struct UnknownModality(pub String);

impl FromStr for Modality {
    type Err = UnknownModality;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Modality::ALL
            .into_iter()
            .find(|m| m.as_str() == s)
            .ok_or_else(|| UnknownModality(s.to_string()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ModalitySet(u8);

impl ModalitySet {
    pub const EMPTY: ModalitySet = ModalitySet(0);

    pub fn contains(self, modality: Modality) -> bool {
        self.0 & modality.bit() != 0
    }

    pub fn insert(&mut self, modality: Modality) {
        self.0 |= modality.bit();
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn to_vec(self) -> Vec<Modality> {
        Modality::ALL.into_iter().filter(|m| self.contains(*m)).collect()
    }

    pub fn from_db(values: &[String]) -> ModalitySet {
        let mut set = ModalitySet::EMPTY;
        for value in values {
            if let Ok(m) = value.parse::<Modality>() {
                set.insert(m);
            }
        }
        set
    }
}

impl FromIterator<Modality> for ModalitySet {
    fn from_iter<I: IntoIterator<Item = Modality>>(iter: I) -> Self {
        let mut set = ModalitySet::EMPTY;
        for m in iter {
            set.insert(m);
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_spelling_round_trips_through_serde_and_from_str() {
        for m in Modality::ALL {
            assert_eq!(serde_json::to_string(&m).unwrap(), format!("\"{}\"", m.as_str()));
            assert_eq!(serde_json::from_str::<Modality>(&format!("\"{}\"", m.as_str())).unwrap(), m);
            assert_eq!(m.as_str().parse::<Modality>().unwrap(), m);
        }
        assert!("playground".parse::<Modality>().is_err());
        assert!(serde_json::from_str::<Modality>("\"Realtime\"").is_err());
    }

    #[test]
    fn set_membership_and_db_parsing() {
        let set = ModalitySet::from_db(&["batch".to_string(), "not-a-thing".to_string()]);
        assert!(set.contains(Modality::Batch));
        assert!(!set.contains(Modality::Realtime));
        assert_eq!(set.to_vec(), vec![Modality::Batch]);
        assert!(ModalitySet::from_db(&[]).is_empty());
        assert_eq!(
            Modality::ALL.into_iter().collect::<ModalitySet>().to_vec(),
            vec![Modality::Realtime, Modality::Batch]
        );
    }
}
