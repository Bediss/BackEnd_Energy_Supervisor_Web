
def intToBin(nbr:int):
    return bin(nbr)

def binToInt(bin:str):
    return int(bin,2)

def intUnion(intList:list):
    setted=set(intList)
    resultUnion=0
    for setElem in setted:
        resultUnion = resultUnion | setElem
    return resultUnion

def binUnion(binUnion:list):
    setted=set(binUnion)
    resultUnion=0
    for setElem in setted:
        resultUnion = resultUnion | binToInt(setElem)
    return intToBin(resultUnion)[2:]

def access(userAccess:int,resourceAccess:int):
    ""
