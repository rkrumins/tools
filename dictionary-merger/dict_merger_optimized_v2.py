from typing import Dict, List, Set, Optional, Tuple, DefaultDict
from collections import defaultdict
from dataclasses import dataclass
import copy
import time

@dataclass(frozen=True)
class EntityLevel:
    """Immutable entity level information"""
    root: str
    level: int
    path_key: str

@dataclass
class LevelCache:
    """Cache for level-based operations"""
    entities: Dict[str, Set[str]]  # root -> entities at this level
    paths: Dict[str, str]  # entity_id -> path_key
    parents: Dict[str, str]  # child -> parent
    root_mapping: Dict[str, str]  # secondary_root -> primary_root

class LevelOptimizedMerger:
    """Graph merger optimized for moderate-depth hierarchies"""
    
    MAX_DEPTH = 6
    
    def __init__(self):
        self.level_caches = [LevelCache(defaultdict(set), {}, {}, {}) 
                           for _ in range(self.MAX_DEPTH)]
    
    def build_level_indices(self, graph: Dict) -> None:
        """Build optimized level-based indices"""
        entities = graph['entities']
        roots = set(graph['roots'])
        
        # Clear existing caches
        for cache in self.level_caches:
            cache.entities.clear()
            cache.paths.clear()
            cache.parents.clear()
            cache.root_mapping.clear()
        
        current_level = [(root, root, 0) for root in roots]
        
        while current_level and current_level[0][2] < self.MAX_DEPTH:
            next_level = []
            level_num = current_level[0][2]
            level_cache = self.level_caches[level_num]
            
            for entity_id, root, _ in current_level:
                if entity_id not in entities:
                    continue
                    
                entity = entities[entity_id]
                level_cache.entities[root].add(entity_id)
                
                children = entity.get('children', [])
                if children and level_num + 1 < self.MAX_DEPTH:
                    for child in children:
                        if child in entities:
                            next_level.append((child, root, level_num + 1))
                            self.level_caches[level_num + 1].parents[child] = entity_id
                
                parent = level_cache.parents.get(entity_id, '')
                path_key = f"{parent}/{entity_id}" if parent else entity_id
                level_cache.paths[entity_id] = path_key
            
            current_level = next_level

    def find_matching_root(self, 
                          root_id: str, 
                          secondary_entities: Dict, 
                          primary_entities: Dict) -> Optional[str]:
        """Find matching root based on structure and properties"""
        if root_id not in secondary_entities:
            return None
            
        secondary_root = secondary_entities[root_id]
        secondary_children = set(secondary_root.get('children', []))
        secondary_props = secondary_root.get('properties', {})
        
        best_match = None
        best_match_score = 0
        
        for primary_root_id in self.level_caches[0].entities.keys():
            if primary_root_id not in primary_entities:
                continue
                
            primary_root = primary_entities[primary_root_id]
            primary_children = set(primary_root.get('children', []))
            primary_props = primary_root.get('properties', {})
            
            # Calculate match score
            score = 0
            # Property matches
            for key, value in secondary_props.items():
                if key in primary_props and primary_props[key] == value:
                    score += 1
            
            # Structure matches
            common_children = len(secondary_children & primary_children)
            score += common_children * 2
            
            if score > best_match_score:
                best_match_score = score
                best_match = primary_root_id
        
        return best_match if best_match_score > 0 else None

    def merge_graphs(self, primary_graph: Dict, secondary_graph: Dict) -> Dict:
        """Merge graphs with root deduplication"""
        try:
            # Initialize merged result
            merged = {
                'entities': {},
                'transitions': primary_graph.get('transitions', {}).copy(),
                'roots': []
            }
            
            # Build indices
            self.build_level_indices(primary_graph)
            primary_caches = list(self.level_caches)
            
            self.build_level_indices(secondary_graph)
            secondary_caches = self.level_caches
            
            # First, process root matching
            root_mappings = {}  # secondary_root -> primary_root
            new_roots = set()
            
            for secondary_root in secondary_graph['roots']:
                matching_root = self.find_matching_root(
                    secondary_root,
                    secondary_graph['entities'],
                    primary_graph['entities']
                )
                
                if matching_root:
                    root_mappings[secondary_root] = matching_root
                else:
                    new_roots.add(secondary_root)
            
            # Copy primary entities
            for entity_id, entity in primary_graph['entities'].items():
                merged['entities'][entity_id] = copy.copy(entity)
            
            # Add primary roots
            merged['roots'].extend(primary_graph['roots'])
            
            # Process secondary entities level by level
            processed_entities = set(merged['entities'].keys())
            batch_updates = defaultdict(dict)
            
            for level in range(self.MAX_DEPTH):
                secondary_cache = secondary_caches[level]
                primary_cache = primary_caches[level]
                
                for secondary_root in secondary_graph['roots']:
                    primary_root = root_mappings.get(secondary_root)
                    entities_at_level = secondary_cache.entities[secondary_root]
                    
                    for entity_id in entities_at_level:
                        if entity_id in processed_entities:
                            continue
                        
                        # If this is a root node, handle it differently
                        if level == 0:
                            if entity_id in root_mappings:
                                # Merge with existing root
                                matching_id = root_mappings[entity_id]
                                batch_updates[matching_id].update({
                                    'properties': {
                                        **secondary_graph['entities'][entity_id].get('properties', {}),
                                        **merged['entities'][matching_id].get('properties', {})
                                    },
                                    'children': list(set(
                                        merged['entities'][matching_id].get('children', []) +
                                        secondary_graph['entities'][entity_id].get('children', [])
                                    ))
                                })
                            elif entity_id in new_roots:
                                # Add as new root
                                merged['entities'][entity_id] = copy.copy(
                                    secondary_graph['entities'][entity_id]
                                )
                                if entity_id not in merged['roots']:
                                    merged['roots'].append(entity_id)
                        else:
                            # Handle non-root entities
                            parent = secondary_cache.parents.get(entity_id)
                            if parent:
                                mapped_parent = root_mappings.get(parent, parent)
                                if mapped_parent in merged['entities']:
                                    # Add as child of mapped parent
                                    merged['entities'][entity_id] = copy.copy(
                                        secondary_graph['entities'][entity_id]
                                    )
                                    if entity_id not in merged['entities'][mapped_parent]['children']:
                                        merged['entities'][mapped_parent]['children'].append(entity_id)
                        
                        processed_entities.add(entity_id)
            
            # Apply batch updates
            for entity_id, updates in batch_updates.items():
                merged['entities'][entity_id].update(updates)
            
            # Process transitions
            existing_transitions = {(t['source'], t['target']) 
                                 for t in merged['transitions'].values()}
            
            for trans_id, trans_data in secondary_graph.get('transitions', {}).items():
                source = trans_data['source']
                target = trans_data['target']
                
                # Map source and target if they were merged with primary entities
                source = root_mappings.get(source, source)
                target = root_mappings.get(target, target)
                
                if ((source in merged['entities']) and 
                    (target in merged['entities']) and 
                    (source, target) not in existing_transitions):
                    
                    new_id = trans_id
                    while new_id in merged['transitions']:
                        new_id = f"{trans_id}_{len(merged['transitions'])}"
                    
                    merged['transitions'][new_id] = {
                        'source': source,
                        'target': target,
                        'properties': trans_data.get('properties', {})
                    }
            
            return merged
            
        except Exception as e:
            print(f"Error during merge: {str(e)}")
            raise

def generate_test_graphs(
    num_roots: int = 10,
    max_depth: int = 6,
    entities_per_level: int = 1000
) -> Tuple[Dict, Dict]:
    """Generate test graphs with controlled depth"""
    from random import randint, choices, sample
    from string import ascii_lowercase, digits
    
    def generate_id() -> str:
        return ''.join(choices(ascii_lowercase + digits, k=6))
    
    def create_graph(shared_entities: Dict = None) -> Dict:
        entities = {}
        if shared_entities:
            entities.update(shared_entities)
        
        # Create roots
        roots = []
        for _ in range(num_roots):
            root_id = generate_id()
            entities[root_id] = {
                "name": f"Root_{root_id}",
                "children": [],
                "properties": {"type": "root", "level": 0}
            }
            roots.append(root_id)
        
        # Create entities level by level
        for level in range(1, max_depth):
            level_entities = []
            for _ in range(entities_per_level):
                entity_id = generate_id()
                entities[entity_id] = {
                    "name": f"Entity_{entity_id}",
                    "children": [],
                    "properties": {"level": level}
                }
                level_entities.append(entity_id)
            
            # Distribute among parents
            previous_level = [eid for eid, e in entities.items() 
                            if e['properties'].get('level') == level - 1]
            
            for entity_id in level_entities:
                parent = sample(previous_level, 1)[0]
                entities[parent]["children"].append(entity_id)
        
        # Add transitions
        transitions = {}
        num_transitions = min(len(entities) // 10, 1000)
        entity_ids = list(entities.keys())
        
        for _ in range(num_transitions):
            source = sample(entity_ids, 1)[0]
            target = sample(entity_ids, 1)[0]
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
    
    # Create secondary with shared entities
    shared = {k: v.copy() for k, v in 
             sample(list(primary['entities'].items()), 
                   len(primary['entities']) // 4)}
    secondary = create_graph(shared)
    
    return primary, secondary

def test_merger_performance():
    """Test the level-optimized merger performance"""
    print("Generating test graphs...")
    start_time = time.time()
    
    primary, secondary = generate_test_graphs(
        num_roots=8,
        max_depth=6,
        entities_per_level=30000
    )
    
    gen_time = time.time() - start_time
    
    print(f"Generation time: {gen_time:.2f} seconds")
    print(f"Primary: {len(primary['entities'])} entities")
    print(f"Secondary: {len(secondary['entities'])} entities")
    
    print("\nMerging graphs...")
    merger = LevelOptimizedMerger()
    start_time = time.time()
    
    try:
        merged = merger.merge_graphs(primary, secondary)
        merge_time = time.time() - start_time
        
        print(f"Merge time: {merge_time:.2f} seconds")
        print(f"Merged: {len(merged['entities'])} entities")
        
        return primary, secondary, merged
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

if __name__ == "__main__":
    test_merger_performance()