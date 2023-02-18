package datastore

import "strings"

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
