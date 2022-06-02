class BankAccount:
    def __init__(self, name, amount=0):
        self.name = name
        self.amount = amount
        print(f'{name}, Your account is now open and has balance of Rs {amount}/-')

    def add_funds(self, amount):
        if amount > 0:
            self.amount += amount
            print(f'{amount} has been added to your balance. Your new account balance is {self.amount}/-')
        else:
            print("Invalid amount")

    def withdraw_funds(self, funds):
        if funds > self.amount:
            raise ValueError("Insufficient Funds")
        else:
            self.amount -= funds
            print(f'{funds} has been withdrawn from your account. Your remaining balance is {self.amount}/-')
            return self.amount



# b1 = BankAccount("Avijeet")
# b1.add_funds(100)
# b1.withdraw_funds(200)
# b1.withdraw_funds(50)


