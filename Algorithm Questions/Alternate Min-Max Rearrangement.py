def modify_array(arr):
    """ 
    :type arr: List[int]
    :rtype: List[int]
    """
    arr.sort()
    new_arr = []
    
    l = 0
    r = len(arr) - 1
    while l < r:
        new_arr.append(arr[l])
        new_arr.append(arr[r])
        l+=1
        r-=1
    
    return new_arr