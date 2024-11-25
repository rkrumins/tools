from typing import Dict, List
import copy
import json
"""
Merge two graph dictionaries while maintaining hierarchical structure.

Args:
    graph1: First graph dictionary
    graph2: Second graph dictionary

Returns:
    Merged graph dictionary
"""
def get_entity_path(graph: Dict, entity_id: str, path: List[str] = None) -> List[str]:
    """
    Get the path from root to the given entity.
    Returns None if entity is not found.
    """
    if path is None:
        path = []

    # Check if entity is a root
    if entity_id in graph['roots']:
        return [entity_id]

    # Check all entities for potential parents
    for ent_id, ent_data in graph['entities'].items():
        if entity_id in ent_data['children']:
            new_path = get_entity_path(graph, ent_id, path)
            if new_path is not None:
                return new_path + [entity_id]

    return None

def generate_unique_id(original_id: str, existing_ids: set) -> str:
    """Generate a unique ID if there's a conflict"""
    if original_id not in existing_ids:
        return original_id

    counter = 1
    while f"{original_id}_{counter}" in existing_ids:
        counter += 1
    return f"{original_id}_{counter}"

# Initialize merged graph
merged = {
    "entities": {},
    "transitions": {},
    "roots": []
}

# Keep track of ID mappings for conflict resolution
id_mappings = {}
existing_ids = set()

# Helper function to add entities with conflict resolution
def add_entity(entity_id: str, entity_data: Dict, source_graph: Dict) -> str:
    if entity_id in id_mappings:
        return id_mappings[entity_id]

    # Generate new ID if needed
    new_id = generate_unique_id(entity_id, existing_ids)
    id_mappings[entity_id] = new_id
    existing_ids.add(new_id)

    # Deep copy the entity data and update children IDs
    new_entity_data = copy.deepcopy(entity_data)
    new_entity_data['children'] = [
        id_mappings.get(child, child)
        for child in entity_data['children']
    ]

    merged['entities'][new_id] = new_entity_data
    return new_id

def merge_graphs(graph1: Dict, graph2: Dict) -> Dict:
    # Process both graphs
    for graph in [graph1, graph2]:
        # First pass: add all entities and maintain ID mappings
        for entity_id, entity_data in graph['entities'].items():
            add_entity(entity_id, entity_data, graph)

        # Add roots with mapped IDs
        for root in graph['roots']:
            new_root_id = id_mappings.get(root, root)
            if new_root_id not in merged['roots']:
                merged['roots'].append(new_root_id)

        # Add transitions with mapped IDs
        for trans_id, trans_data in graph['transitions'].items():
            new_trans_id = generate_unique_id(trans_id,
                                           set(merged['transitions'].keys()))

            merged['transitions'][new_trans_id] = {
                'source': id_mappings.get(trans_data['source'],
                                        trans_data['source']),
                'target': id_mappings.get(trans_data['target'],
                                        trans_data['target'])
            }

    return merged

# Example usage and testing
def test_graph_merger():
    # Test graph 1
    graph1 = {
        "entities": {
            "abc1": {"name": "Test 1", "children": []},
            "abc2": {"name": "Test 2", "children": []},
            "abc3": {"name": "Parent 1", "children": ["abc1", "abc2"]}
        },
        "transitions": {
            "t1": {"source": "abc1", "target": "abc2"}
        },
        "roots": ["abc3"]
    }

    # Test graph 2 with some overlapping IDs
    graph2 = {
        "entities": {
            "abc1": {"name": "Different Test 1", "children": []},
            "xyz1": {"name": "New Node", "children": ["abc1"]}
        },
        "transitions": {
            "t2": {"source": "abc1", "target": "xyz1"}
        },
        "roots": ["xyz1"]
    }

    merged = merge_graphs(graph1, graph2)
    return merged

if __name__ == "__main__":
    result = test_graph_merger()
    print("Merged graph:", result)
    print(json.dumps(result))
