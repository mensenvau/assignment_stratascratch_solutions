def count_friends(user_ids):
    """
    :type user_ids: List[List[int]]
    :rtype: Dict[int, int]
    """
    dic = {}
    for r in user_ids:
        for user_ids in r:
            if user_ids not in dic:
                dic[user_ids] = 0
            dic[user_ids] +=1
    return dic