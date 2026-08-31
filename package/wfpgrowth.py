import itertools
from loguru import logger
# logger = logging.getLogger()

"""
weighted FP-Growth: input: (pattern list, weight), example:
transactions = [([1, 2, 5],100),
                ([2, 4],49.57),
                ([2, 3],134.38),
                ([1, 2, 4],38.22),
                ([1, 3],78),
                ([2, 3],56),
                ([1, 3],25),
                ([1, 2, 3, 5],50),
                ([1, 2, 3],35)]
patterns = find_frequent_patterns(transactions, min_support)
origin: @evandempsey pyfpgrowth
update: @shitohuang
"""
class FPNode(object):
    """
    A node in the FP tree.
    """

    def __init__(self, value, count, parent):
        """
        Create the node.
        """
        self.value = value
        self.count = count
        self.parent = parent
        self.link = None
        self.children = []

    def has_child(self, value):
        """
        Check if node has a particular child node.
        """
        for node in self.children:
            if node.value == value:
                return True

        return False

    def get_child(self, value):
        """
        Return a child node with a particular value.
        """
        for node in self.children:
            if node.value == value:
                return node

        return None

    def add_child(self, value):
        """
        Add a node as a child node.
        """
        child = FPNode(value, 1, self)
        self.children.append(child)
        return child


class FPTree(object):
    """
    A frequent pattern tree.
    """

    def __init__(self, transactions, threshold, max_combination, root_value, root_count):
        """
        Initialize the tree.
        """
        self.frequent = self.find_frequent_items(transactions, threshold)
        self.headers = self.build_header_table(self.frequent)
        self.max_combination = max_combination
        self.root = self.build_fptree(
            transactions, root_value,
            root_count, self.frequent, self.headers)

    @staticmethod
    def find_frequent_items(transactions, threshold):
        """
        Create a dictionary of items with occurrences above the threshold.
        """
        items = {}
        for transaction in transactions:
            for item in transaction[0]:
                if item in items:
                    items[item] += transaction[1]
                else:
                    items[item] = transaction[1]

        for key in list(items.keys()):
            if items[key] < threshold:
                del items[key]

        return items

    @staticmethod
    def build_header_table(frequent):
        """
        Build the header table.
        """
        headers = {}
        for key in frequent.keys():
            headers[key] = None

        return headers

    def build_fptree(self, transactions, root_value,
                     root_count, frequent, headers):
        """
        Build the FP tree and return the root node.
        """
        root = FPNode(root_value, root_count, None)

        for transaction in transactions:
            sorted_items = [x for x in transaction[0] if x in frequent]
            sorted_items.sort(key=lambda x: frequent[x], reverse=True)
            if len(sorted_items) > 0:
                self.insert_tree(sorted_items,transaction[1], root, headers)

        return root

    def insert_tree(self, items,weight, node, headers):
        """
        Recursively grow FP tree.
        """
        first = items[0]
        child = node.get_child(first)
        if child is not None:
            child.count += weight
        else:
            # Add new child.
            child = node.add_child(first)

            # Link it to header structure.
            if headers[first] is None:
                headers[first] = child
            else:
                current = headers[first]
                while current.link is not None:
                    current = current.link
                current.link = child

        # Call function recursively.
        remaining_items = items[1:]
        if len(remaining_items) > 0:
            self.insert_tree(remaining_items,weight, child, headers)

    def tree_has_single_path(self, node):
        """
        If there is a single path in the tree,
        return True, else return False.
        """
        num_children = len(node.children)
        if num_children > 1:
            return False
        elif num_children == 0:
            return True
        else:
            return True and self.tree_has_single_path(node.children[0])

    def mine_patterns(self,transactions, threshold):
        """
        Mine the constructed FP tree for frequent patterns.
        """
        if self.tree_has_single_path(self.root):
            return self.generate_pattern_list()
        else:
            return self.zip_patterns(self.mine_sub_trees(transactions,threshold))

    def zip_patterns(self, patterns):
        """
        Append suffix to patterns in dictionary if
        we are in a conditional FP tree.
        """
        suffix = self.root.value

        if suffix is not None:
            # We are in a conditional tree.
            new_patterns = {}
            for key in patterns.keys():
                new_patterns[tuple(sorted(list(key) + [suffix]))] = patterns[key]

            return new_patterns

        return patterns

    def generate_pattern_list(self):
        """
        Generate a list of patterns with support counts.
        """
        patterns = {}
        items = self.frequent.keys()

        # If we are in a conditional tree,
        # the suffix is a pattern on its own.
        if self.root.value is None:
            suffix_value = []
        else:
            suffix_value = [self.root.value]
            patterns[tuple(suffix_value)] = self.root.count
        
        cnt = 0 
        for i in range(1, len(items) + 1):
            for subset in itertools.combinations(items, i):
                pattern = tuple(sorted(list(subset) + suffix_value))
                patterns[pattern] = \
                    min([self.frequent[x] for x in subset])
                cnt+=1
                if cnt > self.max_combination:
                    logger.info("trigger max combination cutoff %d"%self.max_combination)
                    break

        return patterns
    
    @staticmethod
    def insect(paths,l,node):
        match_path = []
        match_count = 0
        for path in paths:
            if len(set(path)-set(l))==0:
                insect_count = len(path)
            else:
                insect_count = 0
            if insect_count>match_count:
                match_count = insect_count
                match_path = path
                
        if len(match_path)>0:
            return_path = [item for item in match_path if item!=node]
        else:
            return_path = []
            
        return return_path
            
        
    def mine_sub_trees(self,transactions,threshold):
        """
        Generate subtrees and mine them for patterns.
        """
        patterns = {}
        mining_order = sorted(self.frequent.keys(),
                              key=lambda x: self.frequent[x])
        
    
        
        # Get items in tree in reverse order of occurrences.
        for item in mining_order:
            suffixes = []
            conditional_tree_input = []
            node = self.headers[item]

            # Follow node links to get a list of
            # all occurrences of a certain item.

            while node is not None:
                suffixes.append(node)
                node = node.link

            

            # For each occurrence of the item, 
            # trace the path back to the root node.
         
            paths = []
            for suffix in suffixes:
                frequency = suffix.count
                path = []
                parent = suffix.parent
                val = []
                while parent.parent is not None:
                    path.append(parent.value)
                    parent = parent.parent

                        
                paths.append(path+[item])


            for transaction in transactions:
                match_path = self.insect(paths,transaction[0],item)
                if len(match_path)>0:
                    conditional_tree_input.append((match_path,transaction[1]))

            # Now we have the input for a subtree,
            # so construct it and grab the patterns.
            
            subtree = FPTree(conditional_tree_input, threshold,self.max_combination,
                             item, self.frequent[item])

            subtree_patterns = subtree.mine_patterns(conditional_tree_input,threshold)

            # Insert subtree patterns into main patterns dictionary.
            for pattern in subtree_patterns.keys():
                if pattern in patterns:
                    patterns[pattern] += subtree_patterns[pattern]
                else:
                    patterns[pattern] = subtree_patterns[pattern]

        return patterns


def find_frequent_patterns(transactions, support_threshold,max_combination):
    """
    Given a set of transactions, find the patterns in it
    over the specified support threshold.
    """
    tree = FPTree(transactions, support_threshold,max_combination, None, None)
    return tree.mine_patterns(transactions,support_threshold)
