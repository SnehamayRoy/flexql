#ifndef FLEXQL_BPLUSTREE_H
#define FLEXQL_BPLUSTREE_H

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

class BPlusTree {
public:
    enum class Mode {
        Lexicographic,
        Numeric
    };

    explicit BPlusTree(size_t order = 16, Mode mode = Mode::Lexicographic);

    bool insert(const std::string &key, size_t value);
    std::optional<size_t> find(const std::string &key) const;
    bool contains(const std::string &key) const;

    std::vector<size_t> scan_greater(const std::string &key, bool inclusive) const;
    std::vector<size_t> scan_less_equal(const std::string &key) const;
    std::vector<size_t> scan_less(const std::string &key) const;

private:
    struct Node {
        bool is_leaf = true;
        std::vector<std::string> keys;
        std::vector<size_t> values;
        std::vector<std::unique_ptr<Node>> children;
        Node *next = nullptr;
    };

    struct SplitResult {
        std::string promoted_key;
        std::unique_ptr<Node> right_node;
    };

    size_t order_;
    Mode mode_;
    std::unique_ptr<Node> root_;

    int compare_keys(const std::string &left, const std::string &right) const;
    const Node *find_leaf(const std::string &key) const;
    Node *find_leaf(const std::string &key);
    std::optional<SplitResult> insert_recursive(Node *node, const std::string &key, size_t value);
};

#endif
