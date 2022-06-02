class A:
    counter=0

    def __init__(self):
        A.increment_counter()

    @staticmethod
    def increment_counter():
        A.counter+=1

    @staticmethod
    def get_counter():
        return A.counter

if __name__ == '__main__':
    obj1 = A();
    obj2 = A();

    print(A.get_counter())