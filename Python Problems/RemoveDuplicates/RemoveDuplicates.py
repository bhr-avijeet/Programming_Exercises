def remove_duplicates(nums):
    k = 0
    dup = []
    for i in range(len(nums)):
        if nums[i] in dup:
            nums.remove(nums[i])
            i += 1
        else:
            k += 1
            dup.append(nums[i])
        print(nums)
    print(k)



remove_duplicates([0,0,1,1,1,2,2,3,3,4])