from typing import Dict, List, Set, Optional, Tuple, DefaultDict
from collections import defaultdict
from dataclasses import dataclass
import copy
import time
import logging
import traceback

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@dataclass
class PathInfo:
    """Immutable path information"""
    entity_id: str
    name_path: str
    level: int
    root_id: str

@dataclass
class LevelCache:
    """Cache for level-based operations"""
    entities: DefaultDict[str, Set[str]]  # root -> entities at this level
    name_paths: Dict[str, str]  # entity_id -> name-based path
    parents: Dict[str, str]  # child -> parent
    id_to_name: Dict[str, str]  # entity_id -> name
    
    def __init__(self):
        self.entities = defaultdict(set)
        self.name_paths = {}
        self.parents = {}
        self.id_to_name = {}
        
    def clear(self):
        self.entities.clear()
        self.name_paths.clear()
        self.parents.clear()
        self.id_to_name.clear()

class LevelOptimizedMerger:
    """Graph merger with enhanced error handling"""
    
    MAX_DEPTH = 6
    
    def __init__(self):
        self.level_caches = [LevelCache() for _ in range(self.MAX_DEPTH)]
        
    def validate_graph(self, graph: Dict, graph_name: str) -> None:
        """Validate graph structure"""
        try:
            if not isinstance(graph, dict):
                raise ValueError(f"{graph_name} must be a dictionary")
            
            required_keys = {'entities', 'transitions', 'roots'}
            missing_keys = required_keys - set(graph.keys())
            if missing_keys:
                raise ValueError(f"{graph_name} missing required keys: {missing_keys}")
            
            if not isinstance(graph['entities'], dict):
                raise ValueError(f"{graph_name} entities must be a dictionary")
            
            for entity_id, entity in graph['entities'].items():
                if not isinstance(entity, dict):
                    raise ValueError(f"Entity {entity_id} in {graph_name} must be a dictionary")
                if 'name' not in entity:
                    raise ValueError(f"Entity {entity_id} in {graph_name} missing required 'name' field")
                if 'children' not in entity:
                    entity['children'] = []
                if not isinstance(entity['children'], list):
                    raise ValueError(f"Entity {entity_id} children in {graph_name} must be a list")
                if 'properties' not in entity:
                    entity['properties'] = {}
                    
            if not isinstance(graph['transitions'], dict):
                raise ValueError(f"{graph_name} transitions must be a dictionary")
                
            if not isinstance(graph['roots'], list):
                raise ValueError(f"{graph_name} roots must be a list")
                
        except Exception as e:
            logger.error(f"Graph validation error: {str(e)}")
            raise
    
    def get_entity_name(self, entity_id: str, entities: Dict) -> str:
        """Get normalized entity name with validation"""
        try:
            if entity_id not in entities:
                logger.warning(f"Entity ID {entity_id} not found in entities")
                return entity_id
            entity = entities[entity_id]
            if 'name' not in entity:
                logger.warning(f"Entity {entity_id} has no name field")
                return entity_id
            return str(entity['name'])
        except Exception as e:
            logger.error(f"Error getting entity name for {entity_id}: {str(e)}")
            return entity_id
    
    def build_level_indices(self, graph: Dict) -> None:
        """Build indices with error handling"""
        try:
            entities = graph['entities']
            roots = set(graph['roots'])
            
            # Clear existing caches
            for cache in self.level_caches:
                cache.clear()
            
            # Process by level
            current_level = []
            for root in roots:
                if root not in entities:
                    logger.warning(f"Root {root} not found in entities")
                    continue
                current_level.append((root, root, 0, self.get_entity_name(root, entities)))
            
            for level_num in range(self.MAX_DEPTH):
                if not current_level:
                    break
                    
                next_level = []
                level_cache = self.level_caches[level_num]
                
                for entity_id, root, _, entity_name in current_level:
                    try:
                        if entity_id not in entities:
                            continue
                            
                        entity = entities[entity_id]
                        level_cache.entities[root].add(entity_id)
                        level_cache.id_to_name[entity_id] = entity_name
                        
                        # Build name-based path
                        parent = level_cache.parents.get(entity_id)
                        if parent:
                            parent_path = self.level_caches[level_num - 1].name_paths.get(parent, '')
                            path = f"{parent_path}/{entity_name}"
                        else:
                            path = entity_name
                            
                        level_cache.name_paths[entity_id] = path
                        
                        # Process children
                        for child in entity.get('children', []):
                            if child in entities:
                                child_name = self.get_entity_name(child, entities)
                                next_level.append((child, root, level_num + 1, child_name))
                                self.level_caches[level_num + 1].parents[child] = entity_id
                                
                    except Exception as e:
                        logger.error(f"Error processing entity {entity_id} at level {level_num}: {str(e)}")
                        continue
                
                current_level = next_level
                
        except Exception as e:
            logger.error(f"Error building indices: {str(e)}")
            raise

    def merge_graphs(self, primary_graph: Dict, secondary_graph: Dict) -> Dict:
        """Merge graphs with comprehensive error handling"""
        try:
            # Validate input graphs
            logger.info("Validating input graphs...")
            self.validate_graph(primary_graph, "Primary graph")
            self.validate_graph(secondary_graph, "Secondary graph")
            
            logger.info("Building indices...")
            # Build indices
            self.build_level_indices(primary_graph)
            primary_caches = copy.deepcopy(self.level_caches)
            
            self.build_level_indices(secondary_graph)
            secondary_caches = self.level_caches
            
            # Initialize merged result
            logger.info("Initializing merged graph...")
            merged = {
                'entities': {},
                'transitions': primary_graph.get('transitions', {}).copy(),
                'roots': []
            }
            
            # Track processed entities
            processed_entities = set()
            entity_mappings = {}  # secondary_id -> primary_id
            
            logger.info("Processing primary entities...")
            # First pass: Process primary entities
            for entity_id, entity in primary_graph['entities'].items():
                merged['entities'][entity_id] = copy.copy(entity)
                processed_entities.add(entity_id)
            
            # Add primary roots
            merged['roots'].extend(primary_graph['roots'])
            
            logger.info("Processing secondary entities...")
            # Second pass: Process secondary entities level by level
            for level in range(self.MAX_DEPTH):
                try:
                    secondary_cache = secondary_caches[level]
                    primary_cache = primary_caches[level]
                    
                    for secondary_root in secondary_graph['roots']:
                        entities_at_level = secondary_cache.entities[secondary_root]
                        
                        for entity_id in entities_at_level:
                            try:
                                if entity_id in processed_entities:
                                    continue
                                    
                                # Find matching path
                                secondary_path = secondary_cache.name_paths.get(entity_id)
                                if not secondary_path:
                                    continue
                                    
                                # Look for match in primary paths
                                matching_id = None
                                for primary_id, primary_path in primary_cache.name_paths.items():
                                    if secondary_path == primary_path:
                                        matching_id = primary_id
                                        break
                                
                                if matching_id and matching_id in merged['entities']:
                                    # Map entity
                                    entity_mappings[entity_id] = matching_id
                                    
                                    # Merge properties
                                    if 'properties' not in merged['entities'][matching_id]:
                                        merged['entities'][matching_id]['properties'] = {}
                                    merged['entities'][matching_id]['properties'].update(
                                        secondary_graph['entities'][entity_id].get('properties', {})
                                    )
                                    
                                    # Merge children
                                    secondary_children = secondary_graph['entities'][entity_id].get('children', [])
                                    if 'children' not in merged['entities'][matching_id]:
                                        merged['entities'][matching_id]['children'] = []
                                    merged['entities'][matching_id]['children'] = list(set(
                                        merged['entities'][matching_id]['children'] +
                                        [entity_mappings.get(c, c) for c in secondary_children]
                                    ))
                                else:
                                    # Add new entity
                                    merged['entities'][entity_id] = copy.copy(
                                        secondary_graph['entities'][entity_id]
                                    )
                                    
                                    # Update children references
                                    merged['entities'][entity_id]['children'] = [
                                        entity_mappings.get(c, c)
                                        for c in merged['entities'][entity_id].get('children', [])
                                    ]
                                    
                                    # Add to roots if necessary
                                    if level == 0 and entity_id in secondary_graph['roots']:
                                        merged['roots'].append(entity_id)
                                
                                processed_entities.add(entity_id)
                                
                            except Exception as e:
                                logger.error(f"Error processing entity {entity_id}: {str(e)}")
                                continue
                                
                except Exception as e:
                    logger.error(f"Error processing level {level}: {str(e)}")
                    continue
            
            logger.info("Processing transitions...")
            # Process transitions
            try:
                existing_transitions = {(t['source'], t['target']) 
                                     for t in merged['transitions'].values()}
                
                for trans_id, trans_data in secondary_graph.get('transitions', {}).items():
                    source = entity_mappings.get(trans_data['source'], trans_data['source'])
                    target = entity_mappings.get(trans_data['target'], trans_data['target'])
                    
                    if ((source in merged['entities']) and 
                        (target in merged['entities']) and 
                        (source, target) not in existing_transitions):
                        
                        new_id = trans_id
                        while new_id in merged['transitions']:
                            new_id = f"{trans_id}_{len(merged['transitions'])}"
                        
                        merged['transitions'][new_id] = {
                            'source': source,
                            'target': target,
                            'properties': trans_data.get('properties', {}).copy()
                        }
            
            except Exception as e:
                logger.error(f"Error processing transitions: {str(e)}")
            
            logger.info("Merge completed successfully")
            return merged
            
        except Exception as e:
            logger.error(f"Error during merge: {str(e)}")
            logger.error(traceback.format_exc())
            raise


def generate_test_graphs(
    num_roots: int = 10,
    max_depth: int = 6,
    entities_per_level: int = 1000
) -> Tuple[Dict, Dict]:
    """Generate test graphs with name-based paths"""
    from random import randint, choices, sample
    from string import ascii_lowercase, digits
    
    def generate_id() -> str:
        return ''.join(choices(ascii_lowercase + digits, k=6))
        
    def generate_name(prefix: str, level: int) -> str:
        return f"{prefix}_{level}_{generate_id()}"
    
    def create_graph(shared_entities: Dict = None) -> Dict:
        entities = {}
        if shared_entities:
            entities.update(shared_entities)
        
        # Create roots with unique names
        roots = []
        for i in range(num_roots):
            root_id = generate_id()
            entities[root_id] = {
                "name": f"Root_{i}_{root_id}",
                "children": [],
                "properties": {"type": "root", "level": 0}
            }
            roots.append(root_id)
        
        # Create entities level by level
        for level in range(1, max_depth):
            level_entities = []
            for i in range(entities_per_level):
                entity_id = generate_id()
                entities[entity_id] = {
                    "name": generate_name(f"Entity", level),
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
    """Test the name-based path merger performance"""
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