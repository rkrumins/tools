from typing import Dict, List, Set, Optional, Tuple, DefaultDict
from collections import defaultdict
from dataclasses import dataclass
import copy
import time
from array import array

@dataclass(frozen=True)
class EntityLevel:
    """Immutable entity level information"""
    root: str
    level: int
    path_key: str  # Optimized path representation

@dataclass
class LevelCache:
    """Cache for level-based operations"""
    entities: Dict[str, Set[str]]  # root -> entities at this level
    paths: Dict[str, str]  # entity_id -> path_key
    parents: Dict[str, str]  # child -> parent

class LevelOptimizedMerger:
    """Graph merger optimized for moderate-depth hierarchies"""
    
    MAX_DEPTH = 6  # Optimization for known maximum depth
    
    def __init__(self):
        # Pre-initialize level caches
        self.level_caches = [LevelCache(defaultdict(set), {}, {}) 
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
        
        # Process by level
        current_level = [(root, root, 0) for root in roots]  # (entity_id, root, level)
        
        while current_level and current_level[0][2] < self.MAX_DEPTH:
            next_level = []
            level_num = current_level[0][2]
            level_cache = self.level_caches[level_num]
            
            for entity_id, root, _ in current_level:
                if entity_id not in entities:
                    continue
                    
                entity = entities[entity_id]
                level_cache.entities[root].add(entity_id)
                
                # Process children
                children = entity.get('children', [])
                if children and level_num + 1 < self.MAX_DEPTH:
                    for child in children:
                        if child in entities:
                            next_level.append((child, root, level_num + 1))
                            self.level_caches[level_num + 1].parents[child] = entity_id
                
                # Optimize path storage
                parent = level_cache.parents.get(entity_id, '')
                path_key = f"{parent}/{entity_id}" if parent else entity_id
                level_cache.paths[entity_id] = path_key
            
            current_level = next_level

    @staticmethod
    def merge_properties(primary: Dict, secondary: Dict) -> Dict:
        """Efficient property merging"""
        if not secondary:
            return primary
        if not primary:
            return secondary
        return {**secondary, **primary}

    def find_matching_entity(
        self,
        entity_id: str,
        level: int,
        source_cache: LevelCache,
        target_cache: LevelCache
    ) -> Optional[str]:
        """Find matching entity at the same level"""
        if level >= self.MAX_DEPTH:
            return None
            
        source_path = source_cache.paths.get(entity_id)
        if not source_path:
            return None
            
        parent = source_cache.parents.get(entity_id)
        if parent:
            parent_match = self.find_matching_entity(
                parent, level - 1,
                source_cache, target_cache
            )
            if not parent_match:
                return None
                
            # Check children of matched parent
            for candidate in target_cache.entities.get(parent_match, set()):
                if target_cache.parents.get(candidate) == parent_match:
                    return candidate
        
        return None

    def merge_graphs(self, primary_graph: Dict, secondary_graph: Dict) -> Dict:
        """Merge graphs with level-based optimization"""
        try:
            # Initialize merged result
            merged = {
                'entities': {},
                'transitions': primary_graph.get('transitions', {}).copy(),
                'roots': primary_graph.get('roots', []).copy()
            }
            
            # Build level indices
            self.build_level_indices(primary_graph)
            primary_caches = list(self.level_caches)  # Save primary caches
            
            self.build_level_indices(secondary_graph)
            secondary_caches = self.level_caches  # Current state is secondary
            
            # Process level by level
            processed_entities = set()
            batch_updates = defaultdict(dict)
            new_entities = {}
            
            # First, copy all primary entities
            for entity_id, entity in primary_graph['entities'].items():
                merged['entities'][entity_id] = copy.copy(entity)
                processed_entities.add(entity_id)
            
            # Process secondary entities level by level
            for level in range(self.MAX_DEPTH):
                secondary_cache = secondary_caches[level]
                primary_cache = primary_caches[level]
                
                # Process each root's entities at this level
                for root in secondary_graph['roots']:
                    entities_at_level = secondary_cache.entities[root]
                    
                    for entity_id in entities_at_level:
                        if entity_id in processed_entities:
                            continue
                            
                        # Try to find matching entity
                        matching_id = self.find_matching_entity(
                            entity_id, level,
                            secondary_cache, primary_cache
                        )
                        
                        if matching_id and matching_id in merged['entities']:
                            # Batch update for existing entity
                            batch_updates[matching_id].update({
                                'properties': self.merge_properties(
                                    merged['entities'][matching_id].get('properties', {}),
                                    secondary_graph['entities'][entity_id].get('properties', {})
                                ),
                                'children': list(set(
                                    merged['entities'][matching_id].get('children', []) +
                                    secondary_graph['entities'][entity_id].get('children', [])
                                ))
                            })
                        else:
                            # Add new entity
                            new_entities[entity_id] = copy.copy(
                                secondary_graph['entities'][entity_id]
                            )
                            if level == 0:  # Root level
                                merged['roots'].append(entity_id)
                        
                        processed_entities.add(entity_id)
            
            # Apply batch updates
            for entity_id, updates in batch_updates.items():
                merged['entities'][entity_id].update(updates)
            
            # Add new entities
            merged['entities'].update(new_entities)
            
            # Process transitions efficiently
            existing_transitions = {(t['source'], t['target']) 
                                 for t in merged['transitions'].values()}
            
            for trans_id, trans_data in secondary_graph.get('transitions', {}).items():
                source, target = trans_data['source'], trans_data['target']
                
                if ((source in merged['entities']) and 
                    (target in merged['entities']) and 
                    (source, target) not in existing_transitions):
                    
                    new_id = trans_id
                    while new_id in merged['transitions']:
                        new_id = f"{trans_id}_{len(merged['transitions'])}"
                    merged['transitions'][new_id] = copy.copy(trans_data)
            
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
        entities_per_level=100000
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