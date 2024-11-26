from typing import Dict, List, Set, Optional, Tuple, DefaultDict
from collections import defaultdict
from dataclasses import dataclass
import copy
import time
import logging
from functools import lru_cache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class EntityInfo:
    """Entity information with path details"""
    id: str
    name: str
    normalized_name: str
    level: int
    path: List[str]
    root_id: str
    parent_id: Optional[str]

class GraphMerger:
    """Optimized graph merger with proper error handling"""
    
    def __init__(self):
        self.entity_info: Dict[str, EntityInfo] = {}
        self.path_map: Dict[str, str] = {}  # path -> entity_id
        self.name_map: DefaultDict[str, Set[str]] = defaultdict(set)  # normalized_name -> set[entity_id]
        self.level_map: DefaultDict[int, Set[str]] = defaultdict(set)  # level -> set[entity_id]
        self.root_map: DefaultDict[str, Set[str]] = defaultdict(set)  # root_id -> set[entity_id]
        self.entity_mappings: Dict[str, str] = {}  # secondary_id -> primary_id
    
    def _clear_state(self):
        """Clear all internal state"""
        self.entity_info.clear()
        self.path_map.clear()
        self.name_map.clear()
        self.level_map.clear()
        self.root_map.clear()
        self.entity_mappings.clear()
    
    @lru_cache(maxsize=1024)
    def _normalize_name(self, name: str) -> str:
        """Normalize entity name for comparison"""
        return str(name).lower().strip()
    
    def _build_entity_info(self, graph: Dict, is_primary: bool = True) -> None:
        """Build entity information and indices"""
        try:
            if not is_primary:
                self._clear_state()
            
            entities = graph['entities']
            roots = set(graph['roots'])
            
            # First pass: Create basic entity info
            for entity_id, entity in entities.items():
                name = entity.get('name', entity_id)
                normalized_name = self._normalize_name(name)
                
                # Determine if entity is a root
                is_root = entity_id in roots
                root_id = entity_id if is_root else None
                
                # Find parent
                parent_id = None
                for pid, parent in entities.items():
                    if entity_id in parent.get('children', []):
                        parent_id = pid
                        break
                
                info = EntityInfo(
                    id=entity_id,
                    name=name,
                    normalized_name=normalized_name,
                    level=0,  # Will be updated in second pass
                    path=[],  # Will be updated in second pass
                    root_id=root_id,
                    parent_id=parent_id
                )
                
                self.entity_info[entity_id] = info
                self.name_map[normalized_name].add(entity_id)
            
            # Second pass: Build paths and levels
            def build_path(entity_id: str, visited: Set[str]) -> Tuple[List[str], int]:
                if entity_id in visited:
                    logger.warning(f"Circular reference detected for entity {entity_id}")
                    return [], 0
                
                visited.add(entity_id)
                info = self.entity_info[entity_id]
                
                if info.parent_id is None:
                    return [info.name], 0
                
                parent_path, parent_level = build_path(info.parent_id, visited)
                return parent_path + [info.name], parent_level + 1
            
            # Process each entity
            for entity_id, info in self.entity_info.items():
                path, level = build_path(entity_id, set())
                info.path = path
                info.level = level
                
                # Update maps
                path_str = '/'.join(path)
                self.path_map[path_str] = entity_id
                self.level_map[level].add(entity_id)
                
                # Update root information
                if info.parent_id is None:
                    info.root_id = entity_id
                else:
                    info.root_id = self.entity_info[info.parent_id].root_id
                self.root_map[info.root_id].add(entity_id)
            
        except Exception as e:
            logger.error(f"Error building entity info: {str(e)}")
            raise
    
    def _find_matching_entity(self, entity_id: str, entity: Dict) -> Optional[str]:
        """Find matching entity in primary graph"""
        try:
            info = self.entity_info.get(entity_id)
            if not info:
                return None
            
            # Try exact path match
            path_str = '/'.join(info.path)
            if path_str in self.path_map:
                return self.path_map[path_str]
            
            # Try matching by name and level
            normalized_name = self._normalize_name(entity.get('name', entity_id))
            potential_matches = self.name_map[normalized_name] & self.level_map[info.level]
            
            if potential_matches:
                # If multiple matches, prefer same root
                if info.root_id:
                    for match_id in potential_matches:
                        if self.entity_info[match_id].root_id == info.root_id:
                            return match_id
                return next(iter(potential_matches))
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding matching entity for {entity_id}: {str(e)}")
            return None
    
    def merge_graphs(self, primary_graph: Dict, secondary_graph: Dict) -> Dict:
        """Merge graphs with optimized processing"""
        try:
            # Initialize result with primary graph
            merged = {
                'entities': {},
                'transitions': {},
                'roots': []
            }
            
            # Build information for primary graph
            logger.info("Processing primary graph...")
            self._build_entity_info(primary_graph, True)
            
            # Copy primary entities
            merged['entities'] = copy.deepcopy(primary_graph['entities'])
            merged['roots'] = copy.deepcopy(primary_graph['roots'])
            
            # Store primary state
            primary_info = copy.deepcopy(self.entity_info)
            primary_path_map = copy.deepcopy(self.path_map)
            
            # Build information for secondary graph
            logger.info("Processing secondary graph...")
            self._build_entity_info(secondary_graph, False)
            
            # Process secondary entities
            logger.info("Merging entities...")
            for entity_id, entity in secondary_graph['entities'].items():
                matching_id = self._find_matching_entity(entity_id, entity)
                
                if matching_id and matching_id in merged['entities']:
                    # Merge with existing entity
                    self.entity_mappings[entity_id] = matching_id
                    
                    # Merge properties
                    merged['entities'][matching_id].setdefault('properties', {})
                    merged['entities'][matching_id]['properties'].update(
                        entity.get('properties', {})
                    )
                    
                    # Merge children
                    merged['entities'][matching_id].setdefault('children', [])
                    merged['entities'][matching_id]['children'].extend(
                        child for child in entity.get('children', [])
                        if child not in merged['entities'][matching_id]['children']
                    )
                else:
                    # Add new entity
                    merged['entities'][entity_id] = copy.deepcopy(entity)
                    if entity_id in secondary_graph['roots']:
                        merged['roots'].append(entity_id)
            
            # Update children references
            logger.info("Updating references...")
            for entity_id, entity in merged['entities'].items():
                updated_children = []
                for child_id in entity.get('children', []):
                    mapped_id = self.entity_mappings.get(child_id, child_id)
                    if mapped_id in merged['entities']:
                        updated_children.append(mapped_id)
                entity['children'] = updated_children
            
            # Process transitions
            logger.info("Processing transitions...")
            merged['transitions'] = copy.deepcopy(primary_graph.get('transitions', {}))
            
            for trans_id, trans_data in secondary_graph.get('transitions', {}).items():
                source = self.entity_mappings.get(trans_data['source'], trans_data['source'])
                target = self.entity_mappings.get(trans_data['target'], trans_data['target'])
                
                if source in merged['entities'] and target in merged['entities']:
                    new_trans_id = trans_id
                    while new_trans_id in merged['transitions']:
                        new_trans_id = f"{trans_id}_{len(merged['transitions'])}"
                    
                    merged['transitions'][new_trans_id] = {
                        'source': source,
                        'target': target,
                        'properties': copy.deepcopy(trans_data.get('properties', {}))
                    }
            
            logger.info("Merge completed successfully")
            return merged
            
        except Exception as e:
            logger.error(f"Error during merge: {str(e)}")
            raise

def test_merger():
    """Test the merger with error handling"""
    # Create test graphs
    primary = {
        "entities": {
            "root1": {
                "name": "Department A",
                "children": ["team1", "team2"],
                "properties": {"location": "NYC"}
            },
            "team1": {
                "name": "Frontend",
                "children": ["emp1"],
                "properties": {"tech": "React"}
            },
            "team2": {
                "name": "Backend",
                "children": ["emp2"],
                "properties": {"tech": "Python"}
            },
            "emp1": {
                "name": "John",
                "children": [],
                "properties": {"role": "developer"}
            },
            "emp2": {
                "name": "Alice",
                "children": [],
                "properties": {"role": "engineer"}
            },
            "root3": {
                "name": "Root 3",
                "children": [],
                "properties": {}
            }
        },
        "transitions": {
            "t1": {"source": "emp1", "target": "emp2", "properties": {}}
        },
        "roots": ["root1", "root3"]
    }
    
    secondary = {
        "entities": {
            "root2": {
                "name": "Department A",
                "children": ["team3"],
                "properties": {"location": "SF"}
            },
            "team3": {
                "name": "Frontend",
                "children": ["emp3"],
                "properties": {"tech": "Vue"}
            },
            "emp3": {
                "name": "Sarah",
                "children": [],
                "properties": {"role": "developer"}
            },
            "emp5": {
                "name": "Alice",
                "children": [],
                "properties": {"role": "engineer", "status": "Online"}
            }
        },
        "transitions": {
            "t2": {"source": "team3", "target": "emp3", "properties": {}},
            "t1": {"source": "emp1", "target": "emp2", "properties": {}}
        },
        "roots": ["root2"]
    }
    
    merger = GraphMerger()
    
    try:
        print("Starting merge process...")
        start_time = time.time()
        
        merged = merger.merge_graphs(primary, secondary)
        
        end_time = time.time()
        print(f"\nMerge completed in {end_time - start_time:.2f} seconds")
        
        print("\nMerged graph structure:")
        for root in merged['roots']:
            def print_tree(entity_id: str, level: int = 0):
                entity = merged['entities'][entity_id]
                print("  " * level + f"- {entity['name']} ({entity})")
                for child in entity.get('children', []):
                    print_tree(child, level + 1)
            
            print_tree(root)
        
        import json
        print(json.dumps(merged))
        return primary, secondary, merged
        
    except Exception as e:
        print(f"Error during test: {str(e)}")
        return None

if __name__ == "__main__":
    test_merger()