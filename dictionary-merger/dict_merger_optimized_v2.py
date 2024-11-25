from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict, deque
import random
import string
import time
import copy
import sys

class GraphValidationError(Exception):
    pass

class GraphMerger:
    @staticmethod
    def validate_entity(entity_data: Dict, entity_id: str) -> Dict:
        """Lightweight entity validation without deep copy"""
        if not isinstance(entity_data, dict):
            raise GraphValidationError(f"Entity {entity_id} must be a dictionary")
        
        entity_data.setdefault('name', f"Entity_{entity_id}")
        entity_data.setdefault('children', [])
        entity_data.setdefault('properties', {})
        
        if not isinstance(entity_data['children'], list):
            entity_data['children'] = []
        if not isinstance(entity_data['properties'], dict):
            entity_data['properties'] = {}
            
        return entity_data

    @staticmethod
    def find_roots(entities: Dict) -> List[str]:
        """Find roots efficiently using sets"""
        all_children = set()
        for entity in entities.values():
            all_children.update(entity.get('children', []))
        return [eid for eid in entities if eid not in all_children]

    @staticmethod
    def build_parent_child_maps(entities: Dict) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        """Build both parent->children and child->parent maps in one pass"""
        parent_to_children = defaultdict(set)
        child_to_parents = defaultdict(set)
        
        for parent_id, entity in entities.items():
            for child in entity.get('children', []):
                parent_to_children[parent_id].add(child)
                child_to_parents[child].add(parent_id)
                
        return parent_to_children, child_to_parents

    @staticmethod
    def find_paths_breadth_first(
        entities: Dict,
        roots: Set[str],
        child_to_parents: Dict[str, Set[str]]
    ) -> Dict[str, List[str]]:
        """Find all paths using breadth-first search"""
        paths = {}
        for root in roots:
            queue = deque([(root, [root])])
            visited = {root}
            
            while queue:
                current, path = queue.popleft()
                paths[current] = path
                
                # Get children directly from entities
                children = set(entities[current].get('children', []))
                for child in children:
                    if child not in visited and child in entities:
                        visited.add(child)
                        queue.append((child, path + [child]))
                        
        return paths

    @classmethod
    def merge(cls, master_graph: Dict, delta_graph: Dict) -> Dict:
        """Merge graphs with optimized memory usage"""
        try:
            # Shallow copy master graph and validate
            merged = {
                'entities': {},
                'transitions': {},
                'roots': []
            }
            
            # Process master entities first
            for ent_id, entity in master_graph['entities'].items():
                merged['entities'][ent_id] = cls.validate_entity(
                    copy.deepcopy(entity), ent_id
                )
            
            # Find roots and build relationship maps for master graph
            merged['roots'] = cls.find_roots(merged['entities'])
            parent_to_children, child_to_parents = cls.build_parent_child_maps(merged['entities'])
            
            # Process delta entities
            for ent_id, entity in delta_graph['entities'].items():
                if ent_id in merged['entities']:
                    # Update existing entity
                    existing = merged['entities'][ent_id]
                    incoming = cls.validate_entity(entity, ent_id)
                    
                    # Merge properties
                    existing['properties'].update(incoming.get('properties', {}))
                    
                    # Merge children
                    new_children = set(existing['children'])
                    new_children.update(incoming.get('children', []))
                    existing['children'] = list(new_children)
                else:
                    # Add new entity
                    merged['entities'][ent_id] = cls.validate_entity(
                        copy.deepcopy(entity), ent_id
                    )
            
            # Update roots after all entities are processed
            merged['roots'] = cls.find_roots(merged['entities'])
            
            # Process transitions efficiently using sets
            seen_transitions = {(t['source'], t['target']) 
                              for t in master_graph['transitions'].values()}
            
            # Copy master transitions
            merged['transitions'] = master_graph['transitions'].copy()
            
            # Add new transitions from delta
            for trans_id, trans in delta_graph['transitions'].items():
                source, target = trans.get('source'), trans.get('target')
                if not (source and target):
                    continue
                    
                pair = (source, target)
                if (pair not in seen_transitions and 
                    source in merged['entities'] and 
                    target in merged['entities']):
                    
                    merged['transitions'][trans_id] = {
                        'source': source,
                        'target': target,
                        'properties': trans.get('properties', {})
                    }
                    seen_transitions.add(pair)
            
            return merged
            
        except Exception as e:
            raise GraphValidationError(f"Error merging graphs: {str(e)}")

def generate_test_graphs(num_entities: int) -> Tuple[Dict, Dict]:
    def generate_id() -> str:
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    
    def create_graph(base_entities: Dict = None, max_depth: int = 3) -> Dict:
        entities = {}
        transitions = {}
        level_entities = defaultdict(list)
        current_level = 0
        
        # Create base entities
        remaining = num_entities
        while remaining > 0:
            batch_size = min(remaining, 1000)  # Process in batches
            for _ in range(batch_size):
                ent_id = generate_id()
                entities[ent_id] = {
                    "name": f"Entity_{ent_id}",
                    "children": [],
                    "properties": {"level": current_level}
                }
                level_entities[current_level].append(ent_id)
            remaining -= batch_size
        
        # Create hierarchy with controlled depth
        for level in range(max_depth - 1):
            for parent_id in level_entities[level]:
                # Add 2-3 children randomly
                num_children = random.randint(2, 3)
                available_children = level_entities[level + 1]
                if available_children:
                    children = random.sample(available_children, 
                                          min(num_children, len(available_children)))
                    entities[parent_id]["children"].extend(children)
        
        # Create transitions (limited number for performance)
        num_transitions = min(num_entities // 10, 1000)
        entity_ids = list(entities.keys())
        for _ in range(num_transitions):
            source = random.choice(entity_ids)
            target = random.choice(entity_ids)
            if source != target:
                trans_id = generate_id()
                transitions[trans_id] = {
                    "source": source,
                    "target": target,
                    "properties": {}
                }
        
        # Find roots
        all_children = {child for e in entities.values() for child in e["children"]}
        roots = [eid for eid in level_entities[0] if eid not in all_children]
        
        return {"entities": entities, "transitions": transitions, "roots": roots}
    
    # Create master and delta graphs
    master = create_graph()
    
    # Create delta with some overlap
    shared_entities = {k: v for k, v in random.sample(list(master['entities'].items()), 
                                                    num_entities // 4)}
    delta = create_graph(shared_entities)
    
    return master, delta

def test_merger_performance():
    num_entities = 100000
    print(f"Generating test graphs with {num_entities} entities...")
    
    start_gen = time.time()
    master, delta = generate_test_graphs(num_entities)
    end_gen = time.time()
    
    print(f"Generation time: {end_gen - start_gen:.2f} seconds")
    print(f"\nMaster graph: {len(master['entities'])} entities, "
          f"{len(master['transitions'])} transitions, {len(master['roots'])} roots")
    print(f"Delta graph: {len(delta['entities'])} entities, "
          f"{len(delta['transitions'])} transitions, {len(delta['roots'])} roots")
    
    print("\nMerging graphs...")
    start_merge = time.time()
    
    try:
        merged = GraphMerger.merge(master, delta)
        end_merge = time.time()
        
        print(f"\nMerged graph: {len(merged['entities'])} entities, "
              f"{len(merged['transitions'])} transitions, {len(merged['roots'])} roots")
        print(f"Merge time: {end_merge - start_merge:.2f} seconds")
        
        return master, delta, merged
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

if __name__ == "__main__":
    test_merger_performance()