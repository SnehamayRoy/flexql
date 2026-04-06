#include "bplustree.h"

#include <algorithm>
#include <cstdlib>

namespace {

bool parse_number(const std::string &value, long double &out) {
    if (value.empty()) {
        return false;
    }
    char *end = nullptr;
    out = std::strtold(value.c_str(), &end);
    return end && *end == '\0';
}

}  // namespace

BPlusTree::BPlusTree(size_t order, Mode mode)
    : order_(std::max<size_t>(order, 4)), mode_(mode), root_(std::make_unique<Node>()) {}

int BPlusTree::compare_keys(const std::string &left, const std::string &right) const {
    if (mode_ == Mode::Numeric) {
        long double left_num = 0;
        long double right_num = 0;
        if (parse_number(left, left_num) && parse_number(right, right_num)) {
            if (left_num < right_num) {
                return -1;
            }
            if (left_num > right_num) {
                return 1;
            }
            return 0;
        }
    }

    if (left < right) {
        return -1;
    }
    if (left > right) {
        return 1;
    }
    return 0;
}

const BPlusTree::Node *BPlusTree::find_leaf(const std::string &key) const {
    const Node *node = root_.get();
    while (node && !node->is_leaf) {
        size_t child_index = 0;
        while (child_index < node->keys.size() && compare_keys(key, node->keys[child_index]) >= 0) {
            ++child_index;
        }
        node = node->children[child_index].get();
    }
    return node;
}

BPlusTree::Node *BPlusTree::find_leaf(const std::string &key) {
    return const_cast<Node *>(static_cast<const BPlusTree *>(this)->find_leaf(key));
}

std::optional<size_t> BPlusTree::find(const std::string &key) const {
    const Node *leaf = find_leaf(key);
    if (!leaf) {
        return std::nullopt;
    }

    for (size_t i = 0; i < leaf->keys.size(); ++i) {
        if (compare_keys(leaf->keys[i], key) == 0) {
            return leaf->values[i];
        }
    }
    return std::nullopt;
}

bool BPlusTree::contains(const std::string &key) const {
    return find(key).has_value();
}

std::optional<BPlusTree::SplitResult> BPlusTree::insert_recursive(Node *node, const std::string &key, size_t value) {
    if (node->is_leaf) {
        size_t pos = 0;
        while (pos < node->keys.size() && compare_keys(node->keys[pos], key) < 0) {
            ++pos;
        }
        if (pos < node->keys.size() && compare_keys(node->keys[pos], key) == 0) {
            return std::nullopt;
        }

        node->keys.insert(node->keys.begin() + static_cast<std::ptrdiff_t>(pos), key);
        node->values.insert(node->values.begin() + static_cast<std::ptrdiff_t>(pos), value);

        if (node->keys.size() < order_) {
            return std::nullopt;
        }

        auto right = std::make_unique<Node>();
        right->is_leaf = true;

        const size_t split = node->keys.size() / 2;
        right->keys.assign(node->keys.begin() + static_cast<std::ptrdiff_t>(split), node->keys.end());
        right->values.assign(node->values.begin() + static_cast<std::ptrdiff_t>(split), node->values.end());

        node->keys.erase(node->keys.begin() + static_cast<std::ptrdiff_t>(split), node->keys.end());
        node->values.erase(node->values.begin() + static_cast<std::ptrdiff_t>(split), node->values.end());

        right->next = node->next;
        node->next = right.get();
        return SplitResult{right->keys.front(), std::move(right)};
    }

    size_t child_index = 0;
    while (child_index < node->keys.size() && compare_keys(key, node->keys[child_index]) >= 0) {
        ++child_index;
    }

    auto child_split = insert_recursive(node->children[child_index].get(), key, value);
    if (!child_split.has_value()) {
        return std::nullopt;
    }

    node->keys.insert(
        node->keys.begin() + static_cast<std::ptrdiff_t>(child_index),
        child_split->promoted_key);
    node->children.insert(
        node->children.begin() + static_cast<std::ptrdiff_t>(child_index + 1),
        std::move(child_split->right_node));

    if (node->keys.size() < order_) {
        return std::nullopt;
    }

    auto right = std::make_unique<Node>();
    right->is_leaf = false;

    const size_t mid = node->keys.size() / 2;
    const std::string promoted = node->keys[mid];

    right->keys.assign(
        node->keys.begin() + static_cast<std::ptrdiff_t>(mid + 1),
        node->keys.end());
    node->keys.erase(
        node->keys.begin() + static_cast<std::ptrdiff_t>(mid),
        node->keys.end());

    right->children.assign(
        std::make_move_iterator(node->children.begin() + static_cast<std::ptrdiff_t>(mid + 1)),
        std::make_move_iterator(node->children.end()));
    node->children.erase(
        node->children.begin() + static_cast<std::ptrdiff_t>(mid + 1),
        node->children.end());

    return SplitResult{promoted, std::move(right)};
}

bool BPlusTree::insert(const std::string &key, size_t value) {
    if (contains(key)) {
        return false;
    }

    auto split = insert_recursive(root_.get(), key, value);
    if (!split.has_value()) {
        return true;
    }

    auto new_root = std::make_unique<Node>();
    new_root->is_leaf = false;
    new_root->keys.push_back(split->promoted_key);
    new_root->children.push_back(std::move(root_));
    new_root->children.push_back(std::move(split->right_node));
    root_ = std::move(new_root);
    return true;
}

std::vector<size_t> BPlusTree::scan_greater(const std::string &key, bool inclusive) const {
    std::vector<size_t> results;
    const Node *node = find_leaf(key);
    if (!node) {
        return results;
    }

    bool first_leaf = true;
    while (node) {
        for (size_t i = 0; i < node->keys.size(); ++i) {
            if (first_leaf) {
                int cmp = compare_keys(node->keys[i], key);
                if ((inclusive && cmp >= 0) || (!inclusive && cmp > 0)) {
                    results.push_back(node->values[i]);
                }
            } else {
                results.push_back(node->values[i]);
            }
        }
        first_leaf = false;
        node = node->next;
    }
    return results;
}

std::vector<size_t> BPlusTree::scan_less_equal(const std::string &key) const {
    std::vector<size_t> results;
    const Node *node = root_.get();
    if (!node) {
        return results;
    }
    while (node && !node->is_leaf) {
        node = node->children.front().get();
    }

    while (node) {
        for (size_t i = 0; i < node->keys.size(); ++i) {
            if (compare_keys(node->keys[i], key) <= 0) {
                results.push_back(node->values[i]);
            } else {
                return results;
            }
        }
        node = node->next;
    }
    return results;
}

std::vector<size_t> BPlusTree::scan_less(const std::string &key) const {
    std::vector<size_t> results;
    const Node *node = root_.get();
    if (!node) {
        return results;
    }
    while (node && !node->is_leaf) {
        node = node->children.front().get();
    }

    while (node) {
        for (size_t i = 0; i < node->keys.size(); ++i) {
            if (compare_keys(node->keys[i], key) < 0) {
                results.push_back(node->values[i]);
            } else {
                return results;
            }
        }
        node = node->next;
    }
    return results;
}
