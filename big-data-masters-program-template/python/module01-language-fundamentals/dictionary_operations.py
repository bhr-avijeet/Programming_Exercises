if __name__ == '__main__':
    dictionary_one = {"apple":"red","grapes":"green"}
    # print(dictionary_one["apple1"])
    print(dictionary_one.get('app'))

    #Iterate
    for k,v in dictionary_one.items():
        print("key:{}, value:{} ".format(k,v))