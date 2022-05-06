def valid_parentheses(s):
    pair_of_brackets = {'(': ')', '[': ']', '{': '}'}
    stack = []

    for ch in s:
        if ch in list(pair_of_brackets.keys()):
            stack.append(ch)
        elif not stack:
            return False
        else:
            if ch != pair_of_brackets.get(stack.pop()):
                return False

    if stack:
        return False
    return True


valid_parentheses('()}')
