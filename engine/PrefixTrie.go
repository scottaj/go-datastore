package engine

import (
	"strings"
)

type trieNode struct {
	value  string
	isKey  bool
	leaves map[string]*trieNode
}

type PrefixTrie struct {
	root      trieNode
	seperator string
}

func NewPrefixTrie() PrefixTrie {
	return PrefixTrie{
		trieNode{
			value: "",
		},
		":",
	}
}

// Add
/**
* Add a key to the trie as a root key and index all other parts of the key delimited by the configured seperator
 */
func (t *PrefixTrie) Add(prefix string) {
	prefixComponents := strings.Split(prefix, t.seperator)
	var currentValue strings.Builder
	currentNode := &t.root

	for i, component := range prefixComponents {
		if i > 0 {
			currentValue.WriteString(":")
		}
		currentValue.WriteString(component)

		if currentNode.leaves == nil {
			currentNode.leaves = map[string]*trieNode{}
		}

		if currentNode.leaves[currentValue.String()] == nil {
			newNode := trieNode{value: currentValue.String()}
			currentNode.leaves[currentValue.String()] = &newNode
			currentNode = &newNode
		} else {
			currentNode = currentNode.leaves[currentValue.String()]
		}
	}

	currentNode.isKey = true
}

// Delete
/**
* Delete an exact key from the trie
*
* If the key has children in the tree, just mark it as no longer being a key.
*
* If the key has parent nodes that are not keys and do not have other children delete those as well
 */
func (t *PrefixTrie) Delete(key string) bool {
	_, anythingDeleted := t.deleteKey(&t.root, key)
	return anythingDeleted
}

func (t *PrefixTrie) DeleteAll(prefix string) bool {
	deleteRoot, anythingDeleted := t.deleteBranch(&t.root, prefix)

	if deleteRoot {
		t.root.leaves = map[string]*trieNode{}
	}

	return anythingDeleted
}

// Find
/**
* Find all keys that start with the provided prefix
*
* Only works on a complete prefix subset bounded by the delimiter. For exampke if you have a key "country:USA:state:MI"
* and a configured delimiter of ":"; then you could find that key with the searches "", "country", and "country:USA"
* but not the searches "cou", "country:", or "country:Canada"
 */
func (t *PrefixTrie) Find(prefix string) []string {
	prefixComponents := strings.Split(prefix, t.seperator)
	var currentValue strings.Builder
	currentNode := &t.root

	if prefix == "" {
		return t.findKeys(currentNode)
	}

	for i, component := range prefixComponents {
		if i > 0 {
			currentValue.WriteString(":")
		}
		currentValue.WriteString(component)

		if currentNode.leaves[currentValue.String()] == nil {
			currentNode = nil
			break
		} else {
			currentNode = currentNode.leaves[currentValue.String()]
		}
	}

	return t.findKeys(currentNode)
}

// findKeys
/**
* Find all child nodes under the provided node that represent complete keys.
*
* Complete keys are nodes that either have no children or have the isKey property set to true
 */
func (t *PrefixTrie) findKeys(node *trieNode) []string {
	var keys []string

	if node == nil {
		return nil
	} else if node.leaves == nil {
		return append(keys, node.value)
	} else {
		if node.isKey {
			keys = append(keys, node.value)
		}

		for _, childNode := range node.leaves {
			keys = append(keys, t.findKeys(childNode)...)
		}

		return keys
	}
}

// deleteKey
/**
* Delete a specific child node of the provided node from the prefixTrie that exactly matches the provided key value
*
* What deleting means depends on whether the matching node has any child nodes.
* If it has child nodes then the `isKey` property of the node is set to false and the node is not removed.
* If it does not have child nodes then the node it removed from the trie and deleted. If the node has any parent nodes
* that have no children except the deleted nodes (or other parents in the tree that fit that definition) then those are
* deleted as well.
*
* Returns two booleans, the first boolean indicates whether this particular call of the method visited a node that
* matched the provided key. This parameter is used for recursion and should not be used directly by a caller.
* The other parameter indicates whether any recursive call of this method below this call visited a node that matched
* the key and was deleted and is for the external caller's use.
 */
func (t *PrefixTrie) deleteKey(node *trieNode, key string) (bool, bool) {
	anythingDeleted := false

	if node == nil {
		return false, false
	} else if node.value == key {
		return true, node.isKey
	} else {
		for _, childNode := range node.leaves {
			deleteKey, anythingDeletedHere := t.deleteKey(childNode, key)
			anythingDeleted = anythingDeleted || anythingDeletedHere
			if deleteKey {
				if childNode.leaves == nil {
					delete(node.leaves, childNode.value)
				} else {
					childNode.isKey = false
				}
			}
		}

		if node.leaves == nil && !node.isKey {
			return true, anythingDeleted
		}
		return false, anythingDeleted
	}
}

// deleteBranch
/**
* Delete a child node under the provided node that exactly matches the provided key prefix, including deleting all
* the children of that node
*
* Returns two booleans, the first boolean indicates whether this particular call of the method visited a node that
* matched the provided key. This parameter is used for recursion and should not be used directly by a caller.
* The other parameter indicates whether any recursive call of this method below this call visited a node that matched
* the key and was deleted and is for the external caller's use.
 */
func (t *PrefixTrie) deleteBranch(node *trieNode, prefix string) (bool, bool) {
	anythingDeleted := false

	if node == nil {
		return false, false
	} else if node.value == prefix {
		return true, true
	} else {
		for _, childNode := range node.leaves {
			deleteBranch, anythingDeletedHere := t.deleteBranch(childNode, prefix)
			anythingDeleted = anythingDeleted || anythingDeletedHere
			if deleteBranch {
				delete(node.leaves, childNode.value)
				break
			}
		}

		return false, anythingDeleted
	}
}
