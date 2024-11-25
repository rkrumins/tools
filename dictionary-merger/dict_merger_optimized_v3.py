from typing import Dict, List, Set, Optional, Tuple, DefaultDict, NamedTuple
from collections import defaultdict
from dataclasses import dataclass
import copy
import time
import logging
from typing import Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class EntityPath:
    """Entity path and relationship information"""
    entity_id: str
    name: str
    full_path: str
    level: int
    parent_id: Optional[str]
    root_id: str

@dataclass
class MergeContext:
    """Context for entity merging"""
    entity_paths: Dict[str, EntityPath]
    path_to_id: Dict[str, str]
    children_map: DefaultDict[str, Set[str]]
    processed_entities: Set[str]
    entity_mappings: Dict[str, str]  # secondary_id -> primary_id
    path_mappings: Dict[str, str]    # secondary_path -> primary_path

class GraphMerger:
    """Complete graph merger with entity deduplication"""
    
    def __init__(self):
        self.primary_context = MergeContext(
            entity_paths={},
            path_to_id={},
            children_map=defaultdict(set),
            processed_entities=set(),
            entity_mappings={},
            path_mappings={}
        )
        self.secondary_context = MergeContext(
            entity_paths={},
            path_to_id={},
            children_map=defaultdict(set),
            processed_entities=set(),
            entity_mappings={},
            path_mappings={}
        )
    
    def normalize_entity_name(self, entity: Dict[str, Any]) -> str:
        """Normalize entity name for consistent matching"""
        return str(entity.get('name', '')).lower().strip()
    
    def build_graph_context(self, graph: Dict, context: MergeContext) -> None:
        """Build complete graph context"""
        context.entity_paths.clear()
        context.path_to_id.clear()
        context.children_map.clear()
        context.processed_entities.clear()
        context.entity_mappings.clear()
        context.path_mappings.clear()
        
        def process_entity(entity_id: str, parent_path: str = "", 
                         parent_id: Optional[str] = None, level: int = 0,
                         root_id: str = None) -> None:
            if entity_id not in graph['entities']:
                return
                
            entity = graph['entities'][entity_id]
            name = self.normalize_entity_name(entity)
            full_path = f"{parent_path}/{name}" if parent_path else name
            
            context.entity_paths[entity_id] = EntityPath(
                entity_id=entity_id,
                name=name,
                full_path=full_path,
                level=level,
                parent_id=parent_id,
                root_id=root_id or entity_id
            )
            
            context.path_to_id[full_path] = entity_id
            
            if parent_id:
                context.children_map[parent_id].add(entity_id)
            
            for child_id in entity.get('children', []):
                if child_id in graph['entities']:
                    process_entity(child_id, full_path, entity_id, 
                                 level + 1, root_id or entity_id)
        
        for root_id in graph['roots']:
            process_entity(root_id)

    def entities_match(self, primary_entity: Dict, secondary_entity: Dict) -> bool:
        """Determine if two entities should be merged"""
        return (self.normalize_entity_name(primary_entity) == 
                self.normalize_entity_name(secondary_entity))

    def merge_entity_properties(self, primary: Dict, secondary: Dict) -> Dict:
        """Merge entity properties and attributes"""
        merged = copy.deepcopy(primary)
        
        # Merge properties dictionary
        if 'properties' not in merged:
            merged['properties'] = {}
        merged['properties'].update(secondary.get('properties', {}))
        
        # Merge children lists (will be processed fully later)
        primary_children = set(primary.get('children', []))
        secondary_children = set(secondary.get('children', []))
        merged['children'] = list(primary_children | secondary_children)
        
        # Preserve name from primary
        if 'name' not in merged and 'name' in secondary:
            merged['name'] = secondary['name']
        
        return merged

    def merge_graphs(self, primary_graph: Dict, secondary_graph: Dict) -> Dict:
        """Merge graphs with complete deduplication"""
        try:
            # Initialize contexts
            self.build_graph_context(primary_graph, self.primary_context)
            self.build_graph_context(secondary_graph, self.secondary_context)
            
            # Initialize merged result
            merged = {
                'entities': {},
                'transitions': {},
                'roots': []
            }
            
            def find_matching_entity(entity_id: str, entity: Dict) -> Optional[str]:
                """Find matching entity in primary graph"""
                # First try path matching
                entity_path = self.secondary_context.entity_paths.get(entity_id)
                if entity_path:
                    if entity_path.full_path in self.primary_context.path_to_id:
                        return self.primary_context.path_to_id[entity_path.full_path]
                    
                    # Try matching parent path + name
                    parent_id = entity_path.parent_id
                    if parent_id:
                        parent_path = self.secondary_context.entity_paths.get(parent_id)
                        if parent_path:
                            for primary_id, primary_path in self.primary_context.entity_paths.items():
                                if (primary_path.parent_id == parent_path.parent_id and
                                    self.entities_match(primary_graph['entities'][primary_id], entity)):
                                    return primary_id
                
                # Try matching by name under same root
                entity_name = self.normalize_entity_name(entity)
                root_id = entity_path.root_id if entity_path else None
                if root_id:
                    for primary_id, primary_path in self.primary_context.entity_paths.items():
                        if (primary_path.root_id == root_id and
                            primary_path.name == entity_name):
                            return primary_id
                
                return None
            
            # First, add all primary entities
            for entity_id, entity in primary_graph['entities'].items():
                merged['entities'][entity_id] = copy.deepcopy(entity)
                self.primary_context.processed_entities.add(entity_id)
            
            # Process secondary entities
            for entity_id, entity in secondary_graph['entities'].items():
                if entity_id in self.secondary_context.processed_entities:
                    continue
                
                matching_id = find_matching_entity(entity_id, entity)
                
                if matching_id and matching_id in merged['entities']:
                    # Merge with existing entity
                    merged['entities'][matching_id] = self.merge_entity_properties(
                        merged['entities'][matching_id],
                        entity
                    )
                    self.secondary_context.entity_mappings[entity_id] = matching_id
                else:
                    # Add as new entity
                    merged['entities'][entity_id] = copy.deepcopy(entity)
                
                self.secondary_context.processed_entities.add(entity_id)
            
            # Update children references
            for entity_id, entity in merged['entities'].items():
                updated_children = []
                for child_id in entity.get('children', []):
                    mapped_id = self.secondary_context.entity_mappings.get(child_id, child_id)
                    if mapped_id in merged['entities']:
                        updated_children.append(mapped_id)
                entity['children'] = list(set(updated_children))
            
            # Process roots
            merged_roots = set(primary_graph['roots'])
            for root_id in secondary_graph['roots']:
                mapped_id = self.secondary_context.entity_mappings.get(root_id, root_id)
                if mapped_id in merged['entities']:
                    merged_roots.add(mapped_id)
            merged['roots'] = list(merged_roots)
            
            # Process transitions
            processed_transitions = set()
            transition_map = {}  # Maps transition signatures to IDs
            
            def get_transition_signature(source: str, target: str) -> str:
                """Create unique signature for transition"""
                mapped_source = self.secondary_context.entity_mappings.get(source, source)
                mapped_target = self.secondary_context.entity_mappings.get(target, target)
                return f"{mapped_source}=>{mapped_target}"
            
            # Process primary transitions
            for trans_id, trans_data in primary_graph.get('transitions', {}).items():
                signature = get_transition_signature(
                    trans_data['source'],
                    trans_data['target']
                )
                merged['transitions'][trans_id] = copy.deepcopy(trans_data)
                transition_map[signature] = trans_id
            
            # Process secondary transitions
            for trans_id, trans_data in secondary_graph.get('transitions', {}).items():
                signature = get_transition_signature(
                    trans_data['source'],
                    trans_data['target']
                )
                
                if signature in transition_map:
                    # Merge properties of existing transition
                    existing_id = transition_map[signature]
                    merged['transitions'][existing_id]['properties'].update(
                        trans_data.get('properties', {})
                    )
                else:
                    # Add new transition with mapped entities
                    mapped_source = self.secondary_context.entity_mappings.get(
                        trans_data['source'],
                        trans_data['source']
                    )
                    mapped_target = self.secondary_context.entity_mappings.get(
                        trans_data['target'],
                        trans_data['target']
                    )
                    
                    if mapped_source in merged['entities'] and mapped_target in merged['entities']:
                        new_trans_id = trans_id
                        while new_trans_id in merged['transitions']:
                            new_trans_id = f"{trans_id}_{len(merged['transitions'])}"
                            
                        merged['transitions'][new_trans_id] = {
                            'source': mapped_source,
                            'target': mapped_target,
                            'properties': copy.deepcopy(trans_data.get('properties', {}))
                        }
                        transition_map[signature] = new_trans_id
            
            return merged
            
        except Exception as e:
            logger.error(f"Error during merge: {str(e)}")
            raise

def generate_test_graphs() -> Tuple[Dict, Dict]:
    """Generate test graphs with overlapping entities"""
    primary = {
        "entities": {
            "dept1": {
                "name": "Engineering",
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
            }
        },
        "transitions": {
            "t1": {
                "source": "emp1",
                "target": "emp2",
                "properties": {"type": "collaboration"}
            }
        },
        "roots": ["dept1"]
    }
    
    secondary = {
        "entities": {
            "dept2": {
                "name": "Engineering",  # Same department
                "children": ["team3"],
                "properties": {"location": "SF"}  # Different location
            },
            "team3": {
                "name": "Frontend",  # Same team name
                "children": ["emp3"],
                "properties": {"tech": "Vue"}  # Different tech
            },
            "emp3": {
                "name": "Sarah",
                "children": [],
                "properties": {"role": "developer"}
            }
        },
        "transitions": {
            "t2": {
                "source": "team3",
                "target": "emp3",
                "properties": {"type": "management"}
            }
        },
        "roots": ["dept2"]
    }
    
    return primary, secondary

def test_merger():
    """Test the merger with overlapping entities"""
    print("Generating test graphs...")
    primary, secondary = generate_test_graphs()
    
    print("\nMerging graphs...")
    merger = GraphMerger()
    
    try:
        merged = merger.merge_graphs(primary, secondary)
        
        print("\nMerged graph structure:")
        for root in merged['roots']:
            def print_tree(entity_id: str, level: int = 0):
                entity = merged['entities'][entity_id]
                print("  " * level + f"- {entity['name']} ({entity_id})")
                for child in entity.get('children', []):
                    print_tree(child, level + 1)
            
            print_tree(root)
        
        return primary, secondary, merged
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

if __name__ == "__main__":
    test_merger()