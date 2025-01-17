from typing import Dict, List, Set, Optional, Tuple, DefaultDict
from collections import defaultdict
from dataclasses import dataclass
import copy
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class EntityInfo:
    """Entity information with complete reference tracking"""
    id: str
    name: str
    normalized_name: str
    level: int
    path: List[str]
    root_id: str
    parent_id: Optional[str]
    referenced_by: Set[str]  # IDs of entities referencing this entity
    references: Set[str]     # IDs of entities this entity references

class ReferenceTracker:
    """Tracks and validates entity references"""
    def __init__(self):
        self.entity_refs: DefaultDict[str, Set[str]] = defaultdict(set)
        self.incoming_transitions: DefaultDict[str, Set[str]] = defaultdict(set)
        self.outgoing_transitions: DefaultDict[str, Set[str]] = defaultdict(set)
    
    def add_reference(self, from_id: str, to_id: str):
        """Track entity reference"""
        self.entity_refs[from_id].add(to_id)
    
    def add_transition(self, trans_id: str, source_id: str, target_id: str):
        """Track transition reference"""
        self.incoming_transitions[target_id].add(trans_id)
        self.outgoing_transitions[source_id].add(trans_id)

class GraphMerger:
    """Graph merger with complete reference integrity"""
    
    def __init__(self):
        self.entity_info: Dict[str, EntityInfo] = {}
        self.path_map: Dict[str, str] = {}
        self.name_map: DefaultDict[str, Set[str]] = defaultdict(set)
        self.level_map: DefaultDict[int, Set[str]] = defaultdict(set)
        self.root_map: DefaultDict[str, Set[str]] = defaultdict(set)
        self.entity_mappings: Dict[str, str] = {}  # secondary_id -> primary_id
        self.reference_tracker = ReferenceTracker()
    
    def _build_reference_tracking(self, graph: Dict) -> None:
        """Build complete reference tracking for the graph"""
        for entity_id, entity in graph['entities'].items():
            # Track child references
            for child_id in entity.get('children', []):
                self.reference_tracker.add_reference(entity_id, child_id)
        
        # Track transition references
        for trans_id, trans_data in graph.get('transitions', {}).items():
            source_id = trans_data['source']
            target_id = trans_data['target']
            self.reference_tracker.add_transition(trans_id, source_id, target_id)
    
    def _validate_references(self, entity_id: str, entity: Dict, merged_entities: Dict) -> Set[str]:
        """Validate and return valid child references"""
        valid_children = set()
        for child_id in entity.get('children', []):
            mapped_id = self.entity_mappings.get(child_id, child_id)
            if mapped_id in merged_entities:
                valid_children.add(mapped_id)
        return valid_children

    def _update_transitions(self, old_id: str, new_id: str, transitions: Dict) -> Dict[str, Dict]:
        """Update transitions for merged entity"""
        updated_transitions = {}
        
        for trans_id, trans_data in transitions.items():
            source = trans_data['source']
            target = trans_data['target']
            
            # Map source and target to new IDs
            if source == old_id:
                source = new_id
            else:
                source = self.entity_mappings.get(source, source)
                
            if target == old_id:
                target = new_id
            else:
                target = self.entity_mappings.get(target, target)
            
            # Only include valid transitions
            if source in self.entity_info and target in self.entity_info:
                updated_transitions[trans_id] = {
                    'source': source,
                    'target': target,
                    'properties': copy.deepcopy(trans_data.get('properties', {}))
                }
        
        return updated_transitions

    def merge_graphs(self, primary_graph: Dict, secondary_graph: Dict) -> Dict:
        """Merge graphs ensuring reference integrity"""
        try:
            # Initialize merged result
            merged = {
                'entities': {},
                'transitions': {},
                'roots': []
            }
            
            # Build reference tracking
            logger.info("Building reference tracking...")
            self._build_reference_tracking(primary_graph)
            self._build_reference_tracking(secondary_graph)
            
            # Process primary graph first
            logger.info("Processing primary graph...")
            merged['entities'] = copy.deepcopy(primary_graph['entities'])
            merged['transitions'] = copy.deepcopy(primary_graph['transitions'])
            merged['roots'] = copy.deepcopy(primary_graph['roots'])
            
            # Track processed entities
            processed_entities = set(merged['entities'].keys())
            
            # Process secondary entities
            logger.info("Processing secondary graph...")
            for entity_id, entity in secondary_graph['entities'].items():
                if entity_id in processed_entities:
                    continue
                
                # Find matching entity in primary
                matching_id = None
                for primary_id, primary_entity in merged['entities'].items():
                    if (self._normalize_name(primary_entity.get('name', '')) == 
                        self._normalize_name(entity.get('name', ''))):
                        matching_id = primary_id
                        break
                
                if matching_id:
                    # Merge with existing entity
                    logger.debug(f"Merging entity {entity_id} into {matching_id}")
                    self.entity_mappings[entity_id] = matching_id
                    
                    # Merge properties
                    merged['entities'][matching_id].setdefault('properties', {})
                    merged['entities'][matching_id]['properties'].update(
                        entity.get('properties', {})
                    )
                    
                    # Validate and update children
                    valid_children = self._validate_references(
                        entity_id, entity, merged['entities']
                    )
                    existing_children = set(merged['entities'][matching_id].get('children', []))
                    merged['entities'][matching_id]['children'] = list(
                        existing_children | valid_children
                    )
                else:
                    # Add new entity
                    logger.debug(f"Adding new entity {entity_id}")
                    merged['entities'][entity_id] = copy.deepcopy(entity)
                    if entity_id in secondary_graph['roots']:
                        merged['roots'].append(entity_id)
                
                processed_entities.add(entity_id)
            
            # Update all references to use new IDs
            logger.info("Updating references...")
            for entity_id, entity in merged['entities'].items():
                # Update children references
                valid_children = self._validate_references(entity_id, entity, merged['entities'])
                entity['children'] = list(valid_children)
            
            # Process transitions with complete reference updating
            logger.info("Processing transitions...")
            updated_transitions = {}
            
            # Process primary transitions first
            for trans_id, trans_data in merged['transitions'].items():
                source = trans_data['source']
                target = trans_data['target']
                
                if source in merged['entities'] and target in merged['entities']:
                    updated_transitions[trans_id] = {
                        'source': source,
                        'target': target,
                        'properties': copy.deepcopy(trans_data.get('properties', {}))
                    }
            
            # Process secondary transitions
            for trans_id, trans_data in secondary_graph.get('transitions', {}).items():
                source = self.entity_mappings.get(trans_data['source'], trans_data['source'])
                target = self.entity_mappings.get(trans_data['target'], trans_data['target'])
                
                if source in merged['entities'] and target in merged['entities']:
                    new_trans_id = trans_id
                    while new_trans_id in updated_transitions:
                        new_trans_id = f"{trans_id}_{len(updated_transitions)}"
                    
                    updated_transitions[new_trans_id] = {
                        'source': source,
                        'target': target,
                        'properties': copy.deepcopy(trans_data.get('properties', {}))
                    }
            
            merged['transitions'] = updated_transitions
            
            # Final validation
            logger.info("Performing final validation...")
            self._validate_merged_graph(merged)
            
            return merged
            
        except Exception as e:
            logger.error(f"Error during merge: {str(e)}")
            raise

    def _validate_merged_graph(self, merged: Dict) -> None:
        """Validate final merged graph integrity"""
        entities = merged['entities']
        transitions = merged['transitions']
        roots = merged['roots']
        
        # Validate entity references
        for entity_id, entity in entities.items():
            for child_id in entity.get('children', []):
                if child_id not in entities:
                    logger.warning(f"Removing invalid child reference {child_id} from {entity_id}")
                    entity['children'].remove(child_id)
        
        # Validate transitions
        invalid_transitions = set()
        for trans_id, trans_data in transitions.items():
            if (trans_data['source'] not in entities or 
                trans_data['target'] not in entities):
                invalid_transitions.add(trans_id)
        
        for trans_id in invalid_transitions:
            logger.warning(f"Removing invalid transition {trans_id}")
            del transitions[trans_id]
        
        # Validate roots
        merged['roots'] = [root for root in roots if root in entities]

def test_merger():
    """Test the merger with reference integrity"""
    # Create test graphs
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
                "name": "Engineering",
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
    
    merger = GraphMerger()
    
    try:
        print("Starting merge process...")
        start_time = time.time()
        
        merged = merger.merge_graphs(primary, secondary)
        
        end_time = time.time()
        print(f"\nMerge completed in {end_time - start_time:.2f} seconds")
        
        # Print merged structure
        print("\nMerged graph structure:")
        for root in merged['roots']:
            def print_tree(entity_id: str, level: int = 0):
                entity = merged['entities'][entity_id]
                print("  " * level + f"- {entity['name']} ({entity_id})")
                for child in entity.get('children', []):
                    print_tree(child, level + 1)
            
            print_tree(root)
        
        # Print transitions
        print("\nMerged transitions:")
        for trans_id, trans_data in merged['transitions'].items():
            source = merged['entities'][trans_data['source']]['name']
            target = merged['entities'][trans_data['target']]['name']
            print(f"{source} -> {target} ({trans_id})")
        
        return merged
        
    except Exception as e:
        print(f"Error during test: {str(e)}")
        return None

if __name__ == "__main__":
    test_merger()
