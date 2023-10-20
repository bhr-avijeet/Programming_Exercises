def remove_duplicates(nums):
    k = 0
    dup = []
    for i in range(len(nums)):
        if nums[i] not in dup:
            dup.append(nums[i])
    print(dup)
remove_duplicates([0,0,1,1,1,2,2,3,3,4])