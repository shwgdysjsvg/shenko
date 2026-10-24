//! Snapcompact — an independently installable ContextPolicy plugin (§12.3.1).
//!
//! Follows OMP `0b5c27a` snapcompact 18.12.6: bounded, normalized archive text
//! (tool output/arguments truncated, `truncated_chars` recorded) is rendered
//! locally into dense PNG frames for a vision model to read back; the head and
//! recent tail stay as text, and no summary model is called. Frames are lossy
//! derivatives: every archived range keeps a pointer to its raw events.
//! Enabled only for a model with real vision input; otherwise it reports
//! Unavailable and the registry falls back to another installed strategy.
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Instant;

use cordis_core::{Context, InjectSpec, Plugin, Service};
use shenko_context_policy::{
    Availability, CONTRACT_VERSION, ContextInput, ContextPolicy, CostReport, Derivative,
    HistoryItem, ItemKind, ModelCaps, PolicyError, PolicyManifest, PolicyRegistry,
    PreparedWindow, WindowEntry, estimate_tokens, install_on, unit_is_unfinished, units,
    unresolved_calls,
};

pub mod render;

pub const POLICY_ID: &str = "snapcompact";

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SnapConfig {
    /// Compact once used tokens exceed this share of the image-request limit.
    pub trigger_ratio: f32,
    /// Leading conversation (after the system prefix) kept as text.
    pub head_tokens: u32,
    /// Share of the image-request limit the recent text tail may use.
    pub tail_ratio: f32,
    pub max_tool_output_chars: usize,
    pub max_tool_args_chars: usize,
    /// Frame width in 8px halfwidth cells; a CJK glyph takes two.
    pub columns: u32,
    /// Text rows (16px each) per frame.
    pub rows_per_frame: u32,
    /// Above this share of glyph-less characters in any one record, the frame
    /// is unreadable.
    pub max_unrenderable_ratio: f32,
    /// Smallest image-request limit at which the strategy is fully available.
    pub min_image_context: u32,
}

impl Default for SnapConfig {
    fn default() -> Self {
        Self {
            trigger_ratio: 0.85,
            head_tokens: 1024,
            tail_ratio: 0.25,
            max_tool_output_chars: 2000,
            max_tool_args_chars: 500,
            columns: 160,
            rows_per_frame: 64,
            max_unrenderable_ratio: 0.02,
            min_image_context: 8192,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapCompact {
    config: SnapConfig,
}

impl SnapCompact {
    pub fn new(config: SnapConfig) -> Self {
        Self { config }
    }

    /// OMP Archive.text equivalent: one bounded, normalized line per event.
    fn archive_text(&self, items: &[HistoryItem], truncated: &mut u64) -> String {
        let mut out = String::new();
        for item in items {
            let (label, limit) = match &item.kind {
                ItemKind::System => ("system".to_string(), usize::MAX),
                ItemKind::User => ("user".to_string(), usize::MAX),
                ItemKind::Assistant { cancelled: false } => ("assistant".to_string(), usize::MAX),
                ItemKind::Assistant { cancelled: true } => {
                    ("assistant CANCELLED, not executed".to_string(), usize::MAX)
                }
                ItemKind::ToolCall { call_id, tool, status, .. } => {
                    (format!("call {call_id} {tool} {status:?}"), self.config.max_tool_args_chars)
                }
                ItemKind::ToolResult { call_id, status } => {
                    (format!("result {call_id} {status:?}"), self.config.max_tool_output_chars)
                }
                ItemKind::Other { event_type } => (event_type.clone(), usize::MAX),
            };
            let normalized: String = item
                .text
                .chars()
                .map(|c| if c == '\n' || !c.is_control() { c } else { ' ' })
                .collect();
            let total = normalized.chars().count();
            out.push_str(&format!("[#{} {label}] ", item.at.seq));
            if total > limit {
                out.extend(normalized.chars().take(limit));
                out.push_str(&format!(" …[{} chars truncated]", total - limit));
                *truncated += (total - limit) as u64;
            } else {
                out.push_str(&normalized);
            }
            out.push('\n');
        }
        out
    }
}

impl ContextPolicy for SnapCompact {
    fn manifest(&self) -> PolicyManifest {
        PolicyManifest {
            id: POLICY_ID.into(),
            version: env!("CARGO_PKG_VERSION").into(),
            contract: CONTRACT_VERSION,
            requires: vec!["vision".into()],
        }
    }

    fn availability(&self, caps: &ModelCaps) -> Availability {
        match caps.image_request_context() {
            None => Availability::Unavailable(format!("model {} has no vision input", caps.model_id)),
            Some(limit) if limit < self.config.min_image_context => Availability::Degraded(format!(
                "image requests limited to {limit} tokens on {}",
                caps.model_id
            )),
            Some(_) => Availability::Available,
        }
    }

    fn should_compact(&self, input: &ContextInput<'_>) -> bool {
        input.caps.image_request_context().is_some_and(|limit| {
            input.used_tokens as f32 > limit as f32 * self.config.trigger_ratio
        })
    }

    fn prepare(&self, input: &ContextInput<'_>) -> Result<PreparedWindow, PolicyError> {
        let (Some(vision), Some(limit)) = (input.caps.vision, input.caps.image_request_context())
        else {
            return Err(PolicyError::Unavailable(format!(
                "model {} has no vision input",
                input.caps.model_id
            )));
        };
        let items = input.items;
        let units = units(items);
        let unresolved = unresolved_calls(items);
        let tokens_of = |u: &std::ops::Range<usize>| -> u32 {
            items[u.start..u.end].iter().map(|i| i.tokens).sum()
        };

        // System prefix, then a text head, as OMP keeps the opening context.
        let mut head_end = units
            .iter()
            .take_while(|u| items[u.start..u.end].iter().all(|i| i.kind == ItemKind::System))
            .count();
        let mut head_tokens = 0;
        while head_end < units.len() && head_tokens + tokens_of(&units[head_end]) <= self.config.head_tokens {
            head_tokens += tokens_of(&units[head_end]);
            head_end += 1;
        }
        // Recent text tail.
        let tail_budget = (limit as f32 * self.config.tail_ratio) as u32;
        let mut tail_start = units.len();
        let mut tail_tokens = 0;
        while tail_start > head_end {
            let tokens = tokens_of(&units[tail_start - 1]);
            if tail_tokens + tokens > tail_budget && tail_start < units.len() {
                break;
            }
            tail_tokens += tokens;
            tail_start -= 1;
        }

        let started = Instant::now();
        let mut entries = Vec::new();
        let mut derivatives = Vec::new();
        let mut cost = CostReport::default();
        let mut span: Vec<usize> = Vec::new();
        let flush = |span: &mut Vec<usize>,
                         entries: &mut Vec<WindowEntry>,
                         derivatives: &mut Vec<Derivative>,
                         cost: &mut CostReport|
         -> Result<(), PolicyError> {
            let (Some(&first), Some(&last)) = (span.first(), span.last()) else {
                return Ok(());
            };
            let archived: Vec<HistoryItem> = span.iter().map(|&i| items[i].clone()).collect();
            let (from, to) = (items[first].at.seq, items[last].at.seq);
            // Judged per record: long tool output must not dilute an unreadable
            // user message below the threshold.
            for item in &archived {
                let total = item.text.chars().filter(|c| !c.is_whitespace()).count().max(1);
                let missing = item.text.chars().filter(|&c| !c.is_whitespace() && !render::has_glyph(c)).count();
                if missing as f32 / total as f32 > self.config.max_unrenderable_ratio {
                    return Err(PolicyError::FrameUnreadable(format!(
                        "{missing} of {total} characters in seq {} have no glyph in the frame font",
                        item.at.seq
                    )));
                }
            }
            let text = self.archive_text(&archived, &mut cost.truncated_chars);
            let frames = render::render(&text, self.config.columns, self.config.rows_per_frame);
            cost.unrenderable_chars += frames.unrenderable;
            let note = format!(
                "[seq {from}..={to} shown as {} rendered frame(s): a lossy derivative of bounded text, not the raw record; read raw events by seq or event id through the history tools]",
                frames.pngs.len()
            );
            let tokens = estimate_tokens(&note);
            entries.push(WindowEntry::Pointer { from, to, note, tokens });
            for frame in frames.pngs {
                let tokens = frame.width.div_ceil(vision.patch_px) * frame.height.div_ceil(vision.patch_px);
                cost.image_bytes += frame.png.len() as u64;
                cost.vision_tokens_estimate += tokens;
                derivatives.push(Derivative::new("image/png", frame.png));
                entries.push(WindowEntry::Image { from, to, derivative: derivatives.len() - 1, tokens });
            }
            span.clear();
            Ok(())
        };
        for (index, unit) in units.iter().enumerate() {
            let keep = index < head_end
                || index >= tail_start
                || unit_is_unfinished(&items[unit.start..unit.end], &unresolved);
            if keep {
                flush(&mut span, &mut entries, &mut derivatives, &mut cost)?;
                entries.extend(
                    items[unit.start..unit.end]
                        .iter()
                        .map(|i| WindowEntry::Verbatim { at: i.at, tokens: i.tokens }),
                );
            } else {
                span.extend(unit.clone());
            }
        }
        flush(&mut span, &mut entries, &mut derivatives, &mut cost)?;
        if derivatives.is_empty() {
            return Err(PolicyError::NoReduction);
        }
        cost.render_micros = started.elapsed().as_micros() as u64;
        cost.source_tokens = items.iter().map(|i| i.tokens).sum();
        cost.window_tokens = entries.iter().map(WindowEntry::tokens).sum();

        let mut carried_calls: Vec<String> = unresolved.into_iter().collect();
        carried_calls.sort();
        Ok(PreparedWindow {
            base: input.base,
            policy: self.manifest(),
            entries,
            carried_calls,
            derivatives,
            cost,
        })
    }
}

/// The installable plugin, independent of budget compact: either can be
/// installed, updated, disposed or replaced without the other.
pub struct SnapCompactPlugin;

impl Plugin for SnapCompactPlugin {
    type Config = SnapConfig;
    type Input = SnapConfig;
    type PrepareError = std::io::Error;
    type ApplyError = std::io::Error;

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(POLICY_ID)
    }

    fn inject(&self) -> InjectSpec {
        InjectSpec::none().require(PolicyRegistry::NAME)
    }

    fn prepare(&self, config: SnapConfig) -> Result<SnapConfig, std::io::Error> {
        if config.columns < 2 || config.rows_per_frame == 0 {
            return Err(std::io::Error::other("snapcompact frames need at least 2 columns and 1 row"));
        }
        Ok(config)
    }

    async fn apply(&self, ctx: Context, input: &SnapConfig) -> Result<(), std::io::Error> {
        install_on(&ctx, Arc::new(SnapCompact::new(*input)))?;
        Ok(())
    }
}
