"""
Write a function to find the longest common prefix string amongst an array of strings.

If there is no common prefix, return an empty string "".



Example 1:

Input: strs = ["flower","flow","flight"]
Output: "fl"
Example 2:

Input: strs = ["dog","racecar","car"]
Output: ""
Explanation: There is no common prefix among the input strings.


Constraints:

1 <= strs.length <= 200
0 <= strs[i].length <= 200
strs[i] consists of only lower-case English letters.
"""


def longest_common_prefix(strs):
    """
    :type strs: List[str]
    :rtype: str
    """
    size = len(strs)
    if size == 0:
        return ""
    elif size == 1:
        return strs[0]

    strs.sort()
    print(strs)

    min_length = min(len(strs[0]), len(strs[size - 1]))

    i = 0

    while i < min_length and (strs[0][i] == strs[size - 1][i]):
        i += 1

    pre = strs[0][:i]
    return pre


longest_common_prefix(["flo", "flow", "flaaaaaaaaaa"])

"""
Solution Trick
As we know, default sorting technique for an array of strings in python is alphabetically sorted.
1.Sort the list
2.Find the minimum length between the first string and last string of the list as the prefix cannot 
be longer than the minimum length.
3. Check the common words between both the strings and save the result by string indexing.
4. Note strs[0][:0] will not give any result as 0th index is not included in the end value of indexing.
"""
