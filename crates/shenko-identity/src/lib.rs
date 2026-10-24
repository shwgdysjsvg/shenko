//! Replaceable identity policy on the pinned native Cordis Plugin/Service lifecycle.
use chrono::Utc;
use shenko_types::{identity::*, ids::EntityId};

pub struct DefaultIdentityPolicy;

/// Upper bounds keep a self-edit from swallowing the context window.
pub const MAX_NAME_CHARS: usize = 64;
pub const MAX_SOUL_CHARS: usize = 2000;
pub const MAX_PERSONALITY_CHARS: usize = 2000;

impl DefaultIdentityPolicy {
    /// Revision 1. A provided name is user-given; otherwise the instance is
    /// unnamed and may name itself later.
    pub fn genesis(agent_id: EntityId, name: Option<&str>) -> IdentityProfile {
        let display_name = name.and_then(|n| clean(n, MAX_NAME_CHARS, true));
        IdentityProfile {
            agent_id,
            revision: 1,
            naming: if display_name.is_some() {
                NamingOrigin::UserGiven
            } else {
                NamingOrigin::Unnamed
            },
            display_name,
            soul: None,
            personality: None,
            source: IdentitySource::Genesis,
            created_at: Utc::now(),
        }
    }

    /// The next revision after applying `change`, or `None` when the change is
    /// empty or identical to the current value.
    pub fn successor(
        current: &IdentityProfile,
        change: &IdentityChange,
        source: IdentitySource,
    ) -> Option<IdentityProfile> {
        let mut next = current.clone();
        match change {
            IdentityChange::Name(v) => {
                let v = clean(v, MAX_NAME_CHARS, true)?;
                if current.display_name.as_deref() == Some(v.as_str()) {
                    return None;
                }
                next.naming = match source {
                    IdentitySource::Subject { .. } => NamingOrigin::SelfChosen,
                    _ => NamingOrigin::UserGiven,
                };
                next.display_name = Some(v);
            }
            IdentityChange::Soul(v) => {
                let v = clean(v, MAX_SOUL_CHARS, false)?;
                if current.soul.as_deref() == Some(v.as_str()) {
                    return None;
                }
                next.soul = Some(v);
            }
            IdentityChange::Personality(v) => {
                let v = clean(v, MAX_PERSONALITY_CHARS, false)?;
                if current.personality.as_deref() == Some(v.as_str()) {
                    return None;
                }
                next.personality = Some(v);
            }
        }
        next.revision = current.revision + 1;
        next.source = source;
        next.created_at = Utc::now();
        Some(next)
    }

    /// Persona section placed after the role/tool contract. SPEC §6 order.
    pub fn persona_section(current: &IdentityProfile) -> String {
        let mut s = String::from("## 你自己（身份 revision ");
        s.push_str(&current.revision.to_string());
        s.push_str("）\n");
        match (&current.display_name, current.naming) {
            (Some(name), NamingOrigin::SelfChosen) => {
                s.push_str(&format!("你的名字是「{name}」，是你自己取的。\n"));
            }
            (Some(name), _) => {
                s.push_str(&format!("你的名字是「{name}」，由用户取的。\n"));
            }
            (None, _) => s.push_str(
                "你还没有名字。Shenko 只是你运行的框架名，不是你的名字。\
                 想好后可以单独一行输出 `[identity:name] 名字` 为自己取名。\n",
            ),
        }
        match &current.soul {
            Some(soul) => s.push_str(&format!("你的灵魂（你自己对处事与自我的认识）：{soul}\n")),
            None => s.push_str("你还没有写下自己的灵魂：一两句关于为人处事和自我认知的话。\n"),
        }
        if let Some(p) = &current.personality {
            s.push_str(&format!("你的性格：{p}\n"));
        }
        s.push_str(
            "这些都属于你，可以随反省改写：单独一行输出 `[identity:name] …`、\
             `[identity:soul] …` 或 `[identity:personality] …`，从下一轮起生效。\
             它们不改变上面的运行合同。",
        );
        s
    }
}

fn clean(value: &str, max_chars: usize, single_line: bool) -> Option<String> {
    let value = if single_line {
        value.lines().next().unwrap_or("")
    } else {
        value
    };
    let value: String = value.trim().chars().take(max_chars).collect();
    let value = value.trim().to_string();
    (!value.is_empty()).then_some(value)
}

impl DefaultIdentityPolicy {
    /// Apply the complete edit against one snapshot, as one revision.
    pub fn revise(
        current: &IdentityProfile,
        changes: &[IdentityChange],
        source: IdentitySource,
    ) -> Option<IdentityProfile> {
        let mut next = current.clone();
        let mut changed = false;
        for change in changes {
            if let Some(profile) = Self::successor(&next, change, source.clone()) {
                next = profile;
                changed = true;
            }
        }
        if !changed {
            return None;
        }
        next.revision = current.revision + 1;
        Some(next)
    }

    pub fn parse_change(field: &str, value: &str) -> Option<IdentityChange> {
        match field {
            "name" => Some(IdentityChange::Name(value.to_owned())),
            "soul" => Some(IdentityChange::Soul(value.to_owned())),
            "personality" => Some(IdentityChange::Personality(value.to_owned())),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn directives_and_naming_origin() {
        let agent = EntityId::new();
        let g = DefaultIdentityPolicy::genesis(agent, Some("  小满\n第二行 "));
        assert_eq!(
            (g.display_name.as_deref(), g.naming, g.revision),
            (Some("小满"), NamingOrigin::UserGiven, 1)
        );
        assert!(
            DefaultIdentityPolicy::successor(
                &g,
                &IdentityChange::Name(String::new()),
                IdentitySource::Operator
            )
            .is_none()
        );
        assert!(
            DefaultIdentityPolicy::successor(
                &g,
                &IdentityChange::Name("小满".into()),
                IdentitySource::Operator
            )
            .is_none()
        );
        let n = DefaultIdentityPolicy::successor(
            &g,
            &IdentityChange::Name("岚".into()),
            IdentitySource::Subject {
                step_id: EntityId::new(),
                step_no: 9,
            },
        )
        .unwrap();
        assert_eq!(
            (n.agent_id, n.revision, n.naming),
            (agent, 2, NamingOrigin::SelfChosen)
        );
    }
}

/// Pure semantic policy. No file, shell, network or model capability is exposed.
/// Replacing it does not replace the stable profile schema or agent identity.
pub trait IdentityPolicy: Send + Sync + 'static {
    fn revision(&self) -> &str;
    fn genesis(&self, agent: EntityId, name: Option<&str>) -> IdentityProfile;
    fn persona_section(&self, profile: &IdentityProfile) -> String;
    fn parse_control(&self, header: &str, value: &str) -> Option<IdentityChange>;
    fn revise(
        &self,
        current: &IdentityProfile,
        changes: &[IdentityChange],
        source: IdentitySource,
    ) -> Option<IdentityProfile>;
}

impl IdentityPolicy for DefaultIdentityPolicy {
    fn revision(&self) -> &str {
        "shenko.identity/default@1"
    }
    fn genesis(&self, agent: EntityId, name: Option<&str>) -> IdentityProfile {
        Self::genesis(agent, name)
    }
    fn persona_section(&self, profile: &IdentityProfile) -> String {
        Self::persona_section(profile)
    }
    fn parse_control(&self, header: &str, value: &str) -> Option<IdentityChange> {
        Self::parse_change(header.strip_prefix("identity:")?, value)
    }
    fn revise(
        &self,
        current: &IdentityProfile,
        changes: &[IdentityChange],
        source: IdentitySource,
    ) -> Option<IdentityProfile> {
        Self::revise(current, changes, source)
    }
}

/// Typed capability published and withdrawn by Cordis, not a second registry.
pub struct IdentityService {
    policy: std::sync::Arc<dyn IdentityPolicy>,
}
impl cordis_core::Service for IdentityService {
    const NAME: &'static str = "shenko.identity/v1";
}
impl IdentityService {
    pub fn new(policy: std::sync::Arc<dyn IdentityPolicy>) -> Self {
        Self { policy }
    }
    pub fn revision(&self) -> &str {
        self.policy.revision()
    }
    pub fn genesis(&self, agent: EntityId, name: Option<&str>) -> IdentityProfile {
        self.policy.genesis(agent, name)
    }
    pub fn persona_section(&self, profile: &IdentityProfile) -> String {
        self.policy.persona_section(profile)
    }
    pub fn parse_control(&self, header: &str, value: &str) -> Option<IdentityChange> {
        self.policy.parse_control(header, value)
    }
    pub fn revise(
        &self,
        current: &IdentityProfile,
        changes: &[IdentityChange],
        source: IdentitySource,
    ) -> Option<IdentityProfile> {
        self.policy.revise(current, changes, source)
    }
}

pub struct IdentityPlugin;
impl cordis_core::Plugin for IdentityPlugin {
    type Config = std::sync::Arc<dyn IdentityPolicy>;
    type Input = std::sync::Arc<IdentityService>;
    type PrepareError = std::convert::Infallible;
    type ApplyError = cordis_core::service::ServicePublishError;
    fn prepare(&self, policy: Self::Config) -> Result<Self::Input, Self::PrepareError> {
        Ok(std::sync::Arc::new(IdentityService::new(policy)))
    }
    async fn apply(
        &self,
        ctx: cordis_core::Context,
        service: &Self::Input,
    ) -> Result<(), Self::ApplyError> {
        let _publication = ctx.provide(service.clone())?;
        Ok(())
    }
}

/// Default seed entry, with the exact same native prepare/apply path as replacements.
pub fn default_plugin() -> cordis_core::PreparedPlugin {
    use cordis_core::Plugin;
    let plugin = IdentityPlugin;
    let input = plugin
        .prepare(std::sync::Arc::new(DefaultIdentityPolicy))
        .expect("infallible policy preparation");
    cordis_core::PreparedPlugin::from_input(plugin, input)
}
