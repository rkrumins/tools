from typing import Dict, List
import copy
import json

def merge_graph_dicts(primary_graph: Dict, secondary_graph: Dict) -> Dict:
    """
    Merge two graph dictionaries with properties support.
    """
    def get_entity_path(graph: Dict, entity_id: str, path: List[str] = None) -> List[str]:
        if path is None:
            path = []
        if entity_id in graph['roots']:
            return [entity_id]
        for ent_id, ent_data in graph['entities'].items():
            if entity_id in ent_data['children']:
                new_path = get_entity_path(graph, ent_id, path)
                if new_path is not None:
                    return new_path + [entity_id]
        return None
    
    def get_all_paths(graph: Dict) -> Dict[str, List[str]]:
        paths = {}
        for entity_id in graph['entities']:
            path = get_entity_path(graph, entity_id)
            if path:
                paths[entity_id] = path
        return paths
    
    def find_equivalent_path(entity_path: List[str], paths_dict: Dict[str, List[str]]) -> str:
        for entity_id, path in paths_dict.items():
            if len(path) == len(entity_path):
                match = True
                for i in range(len(path) - 1):
                    if path[i] != entity_path[i]:
                        match = False
                        break
                if match:
                    return entity_id
        return None

    def merge_properties(primary_props: Dict, secondary_props: Dict) -> Dict:
        """Merge properties with primary taking precedence"""
        merged_props = copy.deepcopy(secondary_props)
        merged_props.update(primary_props)
        return merged_props

    # Initialize merged graph
    merged = copy.deepcopy(primary_graph)
    
    # Ensure properties exist in all entities and transitions
    for ent_id in merged['entities']:
        if 'properties' not in merged['entities'][ent_id]:
            merged['entities'][ent_id]['properties'] = {}
            
    for trans_id in merged['transitions']:
        if 'properties' not in merged['transitions'][trans_id]:
            merged['transitions'][trans_id]['properties'] = {}

    primary_paths = get_all_paths(primary_graph)
    secondary_paths = get_all_paths(secondary_graph)
    processed_entities = set(merged['entities'].keys())

    # Process secondary graph
    for entity_id, entity_data in secondary_graph['entities'].items():
        if 'properties' not in entity_data:
            entity_data['properties'] = {}
            
        if entity_id in processed_entities:
            # Merge properties for existing entities
            merged['entities'][entity_id]['properties'] = merge_properties(
                merged['entities'][entity_id].get('properties', {}),
                entity_data.get('properties', {})
            )
            continue
            
        entity_path = secondary_paths.get(entity_id)
        if not entity_path:
            continue
            
        equivalent_id = find_equivalent_path(entity_path, primary_paths)
        
        if equivalent_id is None:
            merged['entities'][entity_id] = copy.deepcopy(entity_data)
            processed_entities.add(entity_id)
            
            if entity_id in secondary_graph['roots']:
                merged['roots'].append(entity_id)

    # Process transitions
    for trans_id, trans_data in secondary_graph['transitions'].items():
        if 'properties' not in trans_data:
            trans_data['properties'] = {}
            
        source = trans_data['source']
        target = trans_data['target']
        
        if source in merged['entities'] and target in merged['entities']:
            new_trans_id = trans_id
            while new_trans_id in merged['transitions']:
                new_trans_id = f"{trans_id}_{len(merged['transitions'])}"
            
            merged['transitions'][new_trans_id] = {
                'source': source,
                'target': target,
                'properties': copy.deepcopy(trans_data.get('properties', {}))
            }

    return merged

def test_with_properties():
    primary = {
        "entities": {
            "root1": {
                "name": "Root 1",
                "children": ["child1"],
                "properties": {"color": "red", "size": 10}
            },
            "child1": {
                "name": "Child 1",
                "children": [],
                "properties": {"type": "basic"}
            }
        },
        "transitions": {
            "t1": {
                "source": "root1",
                "target": "child1",
                "properties": {"weight": 1}
            }
        },
        "roots": ["root1"]
    }
    
    secondary = {
        "entities": {
            "root1": {
                "name": "Root 1",
                "children": ["child2"],
                "properties": {"color": "blue", "height": 20}
            },
            "child2": {
                "name": "Child 2",
                "children": [],
                "properties": {"type": "advanced"}
            }
        },
        "transitions": {
            "t1": {
                "source": "root1",
                "target": "child2",
                "properties": {"weight": 2}
            }
        },
        "roots": ["root1"]
    }
    
    return merge_graph_dicts(primary, secondary)

if __name__ == "__main__":
    result = test_with_properties()
    print("Merged graph:", result)
    print(json.dumps(result))
