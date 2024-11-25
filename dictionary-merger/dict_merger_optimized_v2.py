from typing import Dict, List, Set, Optional, Tuple, DefaultDict
from collections import defaultdict, deque
import copy
import time
import sys
from dataclasses import dataclass
from itertools import chain

@dataclass
class PathInfo:
    """Stores path information for an entity"""
    full_path: List[str]
    path_str: str
    depth: int

class OptimizedGraphMerger:
    """High-performance path-based graph merger"""
    
    @staticmethod
    def build_indices(
        entities: Dict,
        roots: List[str]
    ) -> Tuple[DefaultDict[str, Set[str]], DefaultDict[str, Set[str]], Dict[str, PathInfo]]:
        """Build optimized lookup indices for the graph"""
        # Initialize indices
        child_to_parent: DefaultDict[str, Set[str]] = defaultdict(set)
        parent_to_children: DefaultDict[str, Set[str]] = defaultdict(set)
        path_index: Dict[str, PathInfo] = {}
        roots_set = set(roots)
        
        # Build parent-child relationships
        for entity_id, entity in entities.items():
            for child in entity.get('children', []):
                child_to_parent[child].add(entity_id)
                parent_to_children[entity_id].add(child)
        
        # Build paths using BFS
        queue = deque((root, [root]) for root in roots_set)
        while queue:
            current_id, current_path = queue.popleft()
            path_str = '/'.join(current_path)
            path_index[current_id] = PathInfo(
                full_path=current_path,
                path_str=path_str,
                depth=len(current_path)
            )
            
            # Add children to queue
            for child in parent_to_children[current_id]:
                if child not in path_index:
                    queue.append((child, current_path + [child]))
        
        return child_to_parent, parent_to_children, path_index

    @staticmethod
    def find_matching_paths(
        target_path_info: PathInfo,
        source_paths: Dict[str, PathInfo],
        depth_index: DefaultDict[int, Set[str]]
    ) -> Optional[str]:
        """Find matching paths efficiently using depth indexing"""
        if target_path_info.depth not in depth_index:
            return None
            
        target_prefix = '/'.join(target_path_info.full_path[:-1])
        for candidate_id in depth_index[target_path_info.depth]:
            candidate_path = source_paths[candidate_id]
            if target_prefix == '/'.join(candidate_path.full_path[:-1]):
                return candidate_id
        return None

    @staticmethod
    def merge_properties(primary: Dict, secondary: Dict) -> Dict:
        """Merge properties efficiently"""
        if not secondary:
            return primary
        if not primary:
            return secondary
        return {**secondary, **primary}

    @classmethod
    def merge_graphs(cls, primary_graph: Dict, secondary_graph: Dict) -> Dict:
        """Optimized path-based graph merger"""
        try:
            # Initialize merged graph with primary
            merged = {
                'entities': {},
                'transitions': primary_graph['transitions'].copy(),
                'roots': primary_graph['roots'].copy()
            }
            
            # Build optimized indices
            primary_c2p, primary_p2c, primary_paths = cls.build_indices(
                primary_graph['entities'],
                primary_graph['roots']
            )
            
            secondary_c2p, secondary_p2c, secondary_paths = cls.build_indices(
                secondary_graph['entities'],
                secondary_graph['roots']
            )
            
            # Build depth indices for faster path matching
            primary_depth_index: DefaultDict[int, Set[str]] = defaultdict(set)
            for ent_id, path_info in primary_paths.items():
                primary_depth_index[path_info.depth].add(ent_id)
            
            # Track processed entities
            processed_entities = set()
            
            # First pass: Process primary entities
            for ent_id, entity in primary_graph['entities'].items():
                merged['entities'][ent_id] = copy.copy(entity)
                processed_entities.add(ent_id)
            
            # Second pass: Process secondary entities
            batch_updates: Dict[str, Dict] = {}
            new_transitions: List[Tuple[str, Dict]] = []
            
            for entity_id, entity_data in secondary_graph['entities'].items():
                if entity_id in processed_entities:
                    continue
                    
                path_info = secondary_paths.get(entity_id)
                if not path_info:
                    continue
                
                matching_id = cls.find_matching_paths(
                    path_info,
                    primary_paths,
                    primary_depth_index
                )
                
                if matching_id and matching_id in merged['entities']:
                    # Update existing entity
                    batch_updates[matching_id] = {
                        'properties': cls.merge_properties(
                            merged['entities'][matching_id].get('properties', {}),
                            entity_data.get('properties', {})
                        ),
                        'children': list(set(chain(
                            merged['entities'][matching_id].get('children', []),
                            entity_data.get('children', [])
                        )))
                    }
                else:
                    # Add new entity
                    merged['entities'][entity_id] = copy.copy(entity_data)
                    processed_entities.add(entity_id)
                    
                    if entity_id in secondary_graph['roots']:
                        merged['roots'].append(entity_id)
            
            # Apply batch updates
            for entity_id, updates in batch_updates.items():
                merged['entities'][entity_id].update(updates)
            
            # Process transitions efficiently
            existing_transitions = {(t['source'], t['target']) 
                                 for t in merged['transitions'].values()}
            
            for trans_id, trans_data in secondary_graph['transitions'].items():
                source, target = trans_data['source'], trans_data['target']
                if ((source in merged['entities']) and 
                    (target in merged['entities']) and 
                    (source, target) not in existing_transitions):
                    
                    new_trans_id = trans_id
                    while new_trans_id in merged['transitions']:
                        new_trans_id = f"{trans_id}_{len(merged['transitions'])}"
                    
                    merged['transitions'][new_trans_id] = copy.copy(trans_data)
                    existing_transitions.add((source, target))
            
            return merged
            
        except Exception as e:
            print(f"Error during merge: {str(e)}")
            raise

def generate_large_test_graphs(num_entities: int = 10000) -> Tuple[Dict, Dict]:
    """Generate large test graphs efficiently"""
    from random import randint, choices, sample
    from string import ascii_lowercase, digits
    
    def generate_id() -> str:
        return ''.join(choices(ascii_lowercase + digits, k=6))
    
    def create_graph(shared_entities: Dict = None) -> Dict:
        entities = {}
        id_to_level = {}
        level_to_ids: DefaultDict[int, List[str]] = defaultdict(list)
        max_depth = min(10, num_entities // 1000)  # Limit depth for large graphs
        
        # Create or copy base entities
        if shared_entities:
            entities = shared_entities.copy()
            for ent_id in shared_entities:
                level = 0
                id_to_level[ent_id] = level
                level_to_ids[level].append(ent_id)
        
        # Create new entities
        while len(entities) < num_entities:
            batch_size = min(1000, num_entities - len(entities))
            for _ in range(batch_size):
                ent_id = generate_id()
                level = randint(0, max_depth)
                entities[ent_id] = {
                    "name": f"Entity_{ent_id}",
                    "children": [],
                    "properties": {"level": level}
                }
                id_to_level[ent_id] = level
                level_to_ids[level].append(ent_id)
        
        # Create hierarchy
        for level in range(1, max_depth + 1):
            for child_id in level_to_ids[level]:
                if level_to_ids[level - 1]:
                    parent_id = sample(level_to_ids[level - 1], 1)[0]
                    entities[parent_id]["children"].append(child_id)
        
        # Identify roots and create minimal transitions
        roots = level_to_ids[0]
        transitions = {}
        
        num_transitions = min(num_entities // 10, 1000)
        for i in range(num_transitions):
            source = sample(list(entities.keys()), 1)[0]
            target = sample(list(entities.keys()), 1)[0]
            if source != target:
                trans_id = generate_id()
                transitions[trans_id] = {
                    "source": source,
                    "target": target,
                    "properties": {}
                }
        
        return {"entities": entities, "transitions": transitions, "roots": roots}
    
    # Create primary graph
    primary = create_graph()
    
    # Create secondary with some shared entities
    shared = {k: v.copy() for k, v in 
             sample(list(primary['entities'].items()), num_entities // 4)}
    secondary = create_graph(shared)
    
    return primary, secondary

def test_merger_performance():
    """Test merger performance with large graphs"""
    num_entities = 10000
    print(f"Generating test graphs with {num_entities} entities...")
    
    start_time = time.time()
    primary, secondary = generate_large_test_graphs(num_entities)
    gen_time = time.time() - start_time
    
    print(f"Generation time: {gen_time:.2f} seconds")
    print(f"Primary: {len(primary['entities'])} entities, "
          f"{len(primary['transitions'])} transitions, "
          f"{len(primary['roots'])} roots")
    print(f"Secondary: {len(secondary['entities'])} entities, "
          f"{len(secondary['transitions'])} transitions, "
          f"{len(secondary['roots'])} roots")
    
    print("\nMerging graphs...")
    start_time = time.time()
    
    try:
        merged = OptimizedGraphMerger.merge_graphs(primary, secondary)
        merge_time = time.time() - start_time
        
        print(f"Merge time: {merge_time:.2f} seconds")
        print(f"Merged: {len(merged['entities'])} entities, "
              f"{len(merged['transitions'])} transitions, "
              f"{len(merged['roots'])} roots")
        
        return primary, secondary, merged
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

if __name__ == "__main__":
    test_merger_performance()