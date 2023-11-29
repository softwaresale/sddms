mod conflict_diagnosis;
mod conflict_graph;
mod conflict_type;

use conflict_graph::ConflictGraph;
use crate::organize::AssociatedActionMap;
use crate::verify::conflict_diagnosis::ConflictDiagnosis;

pub fn verify_action_history(associated_action_map: &AssociatedActionMap) -> Result<(), Vec<ConflictDiagnosis>> {

    let all_transaction_ids = associated_action_map.get_all_transaction_ids();

    let conflict_graph = ConflictGraph::new(all_transaction_ids)
        .build(&associated_action_map);

    let cycles = conflict_graph.detect_cycles();
    if cycles.is_empty() {
        Ok(())
    } else {
        Err(cycles.into_iter()
            .map(|cycle| ConflictDiagnosis::new(cycle, &conflict_graph, associated_action_map))
            .collect())
    }
}
