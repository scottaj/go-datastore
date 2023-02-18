package datastore

import (
	"golang.org/x/exp/slices"
	"testing"
)

func TestAddNodesWithNoSeparator(t *testing.T) {
	trie := NewPrefixTrie()

	node1 := "country"
	trie.Add(node1)

	if len(trie.root.leaves) != 1 || trie.root.leaves[node1].value != node1 {
		t.Fatalf("expected one leaf on root node with value %q but found %d: %p", node1, len(trie.root.leaves), trie.root.leaves)
	}

	node2 := "department"
	trie.Add(node2)
	if len(trie.root.leaves) != 2 || trie.root.leaves[node2].value != node2 {
		t.Fatalf("expected two leaves on root node with value %q present but found %d: %p", node2, len(trie.root.leaves), trie.root.leaves)
	}
}

func TestAddSingleNodeWithSeparators(t *testing.T) {
	trie := NewPrefixTrie()

	node1 := "country:USA:state:MI:city:China"
	trie.Add(node1)

	moreNodes := true
	currentNode := &trie.root
	i := 0
	expectedNodeValues := []string{
		"",
		"country",
		"country:USA",
		"country:USA:state",
		"country:USA:state:MI",
		"country:USA:state:MI:city",
		"country:USA:state:MI:city:China",
	}
	for moreNodes {
		currentValue := currentNode.value
		numberOfLeaves := len(currentNode.leaves)
		expectedNodeValue := expectedNodeValues[i]

		if currentValue != expectedNodeValue {
			t.Fatalf("Expected current node %p to have value %q but was %q", currentNode, expectedNodeValue, currentValue)
		}

		if i < len(expectedNodeValues)-1 && numberOfLeaves != 1 {
			t.Fatalf("expected a single leaf on node %p but found %d", currentNode, numberOfLeaves)
		}

		moreNodes = numberOfLeaves > 0
		if moreNodes {
			currentNode = currentNode.leaves[expectedNodeValues[i+1]]
			i++
		}
	}
}

func TestAddMultiBranchKeysWithSharedPrefix(t *testing.T) {
	trie := NewPrefixTrie()

	node1 := "country:USA:state:MI:city:China"
	node2 := "country:USA:state:OH:city:Sandusky"
	trie.Add(node1)
	trie.Add(node2)

	if len(trie.root.leaves) != 1 {
		t.Fatalf("Expected one branch from root because of shared prefix but found %d", len(trie.root.leaves))
	}

	leaves := collectLeaves(&trie.root)
	if len(leaves) != 2 {
		t.Fatalf("expected two leaves from root, but found %d: %v", len(leaves), leaves)
	}
}

func TestAddMultiPrefixesWhereOneIsAFullSubset(t *testing.T) {
	trie := NewPrefixTrie()

	node1 := "country:USA:state:MI"
	node2 := "country:USA:state:MI:city:China"
	trie.Add(node1)
	trie.Add(node2)

	if len(trie.root.leaves) != 1 {
		t.Fatalf("Expected one branch from root because of shared prefix but found %d", len(trie.root.leaves))
	}

	leaves := collectLeaves(&trie.root)
	if len(leaves) != 1 {
		t.Fatalf("expected one leaf from root, but found %d: %v", len(leaves), leaves)
	}
}

func TestAddMultiPrefixesWhereOneIsAFullSubsetAddedAfterLargerChild(t *testing.T) {
	trie := NewPrefixTrie()

	node1 := "country:USA:state:MI:city:Chine"
	node2 := "country:USA:state:MI"
	trie.Add(node1)
	trie.Add(node2)

	if len(trie.root.leaves) != 1 {
		t.Fatalf("Expected one branch from root because of shared prefix but found %d", len(trie.root.leaves))
	}

	leaves := collectLeaves(&trie.root)
	if len(leaves) != 1 {
		t.Fatalf("expected one leaf from root, but found %d: %v", len(leaves), leaves)
	}
}

func TestFindRootKeysByPrefix(t *testing.T) {
	trie := NewPrefixTrie()

	node1 := "country:USA:state:MI:city:China"
	node2 := "country:USA:state:OH:city:Sandusky"
	node3 := "country:USA:state:MI:city:St. Clair"
	node4 := "country:USA:state:OH:city:Cleveland"
	node5 := "country:USA:state:IN:city:Gary"
	trie.Add(node1)
	trie.Add(node2)
	trie.Add(node3)
	trie.Add(node4)
	trie.Add(node5)

	allNodes := trie.Find("")
	if len(allNodes) != 5 || !slices.Contains(allNodes, node1) || !slices.Contains(allNodes, node2) || !slices.Contains(allNodes, node3) || !slices.Contains(allNodes, node4) || !slices.Contains(allNodes, node5) {
		t.Fatalf("expected %v to be length 5 and contain %q, %q, %q, %q, %q", allNodes, node1, node2, node3, node4, node5)
	}

	allUSANodes := trie.Find("country:USA")
	if len(allUSANodes) != 5 || !slices.Contains(allUSANodes, node1) || !slices.Contains(allUSANodes, node2) || !slices.Contains(allUSANodes, node3) || !slices.Contains(allUSANodes, node4) || !slices.Contains(allUSANodes, node5) {
		t.Fatalf("expected %v to be length 5 and contain %q, %q, %q, %q, %q", allUSANodes, node1, node2, node3, node4, node5)
	}

	allOhioNodes := trie.Find("country:USA:state:OH")
	if len(allOhioNodes) != 2 || !slices.Contains(allOhioNodes, node2) || !slices.Contains(allOhioNodes, node4) {
		t.Fatalf("expected %v to be length 2 and contain %q, %q", allOhioNodes, node2, node4)
	}

	chinaMichiganNode := trie.Find("country:USA:state:MI:city:China")
	if len(chinaMichiganNode) != 1 || !slices.Contains(chinaMichiganNode, node1) {
		t.Fatalf("expected %v to be length 1 and contain %q", chinaMichiganNode, node1)
	}
}

func TestFindKeysThatHaveOtherKeysNestedUnderThem(t *testing.T) {
	trie := NewPrefixTrie()

	node1 := "country:USA:state:MI:city:Chine"
	node2 := "country:USA:state:MI"
	node3 := "country:USA:state:OH:city:Sandusky"
	node4 := "country:USA:state:MI:city:St. Clair"
	trie.Add(node1)
	trie.Add(node2)
	trie.Add(node3)
	trie.Add(node4)

	allNodes := trie.Find("")
	if len(allNodes) != 4 || !slices.Contains(allNodes, node1) || !slices.Contains(allNodes, node2) || !slices.Contains(allNodes, node3) || !slices.Contains(allNodes, node4) {
		t.Fatalf("expected %v to be length 4 and contain %q, %q, %q, %q", allNodes, node1, node2, node3, node4)
	}

	allMichiganNodes := trie.Find("country:USA:state:MI")
	if len(allMichiganNodes) != 3 || !slices.Contains(allMichiganNodes, node1) || !slices.Contains(allMichiganNodes, node2) || !slices.Contains(allMichiganNodes, node4) {
		t.Fatalf("expected %v to be length 3 and contain %q, %q, %q", allMichiganNodes, node1, node2, node4)
	}
}

// test (not) finding incomplete prefixes
func TestTryToFindWithIncompletePrefix(t *testing.T) {
	trie := NewPrefixTrie()

	node1 := "country:USA:state:MI:city:China"
	node2 := "country:USA:state:MI"
	node3 := "country:USA:state:OH:city:Sandusky"
	node4 := "country:USA:state:MI:city:St. Clair"
	trie.Add(node1)
	trie.Add(node2)
	trie.Add(node3)
	trie.Add(node4)

	noNodes := trie.Find("c")
	if noNodes != nil {
		t.Fatalf("expected search to not find anything but found %v", noNodes)
	}

	noNodes = trie.Find("country:USA:stat")
	if noNodes != nil {
		t.Fatalf("expected search to not find anything but found %v", noNodes)
	}

	noNodes = trie.Find("country:USB")
	if noNodes != nil {
		t.Fatalf("expected search to not find anything but found %v", noNodes)
	}

	noNodes = trie.Find("country:USA:")
	if noNodes != nil {
		t.Fatalf("expected search to not find anything but found %v", noNodes)
	}
}

func collectLeaves(node *trieNode) []trieNode {
	var leaves []trieNode

	if node.leaves == nil {
		return append(leaves, *node)
	} else {
		for _, v := range node.leaves {
			leaves = append(leaves, collectLeaves(v)...)
		}

		return leaves
	}
}
