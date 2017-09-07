import sys

class database():
    def __init__(self):
        self.database = {}
        self.transactions = [{}] # array of dictionaries
        self.beginTransaction = False

    def set(self, key, value):
        if self.beginTransaction == False:
            self.database[key] = value

        else:
            data = {}
            data[key] = value
            self.transactions.append(data)
            self.beginTransaction = False

    def get(self, key):
        if not self.transactions:
            if key not in self.database:
                print "> NULL"
            else:
                print "> " + str(self.database[key])

        else:
            current_database = self.updateDB()
            if current_database:
                for data in current_database:
                    if key in data and current_database[key] is not None:
                        print "> " + str(current_database[key])
                    else:
                        print "> NULL"
            else:
                print "> NULL"


    def unset(self, keyToDel):
        keyToDel = keyToDel.split("\n")[0]
        if self.beginTransaction == False:
            self.database.pop(keyToDel)

        else: # else add to transactions like set
            data = {}
            data[keyToDel] = None
            self.transactions.append(data)
            self.beginTransaction = False

    def numEqualTo(self, value):
        count = 0
        if not self.transactions: # or self.hasEmptyTransaction() == False:
            for key in self.database:
                if self.database[key] == value:
                    count += 1
        else:
            tempDB = self.updateDB()
            for key in tempDB:
                if tempDB[key] == value:
                    count += 1
        print "> " + str(count)

    def begin(self):
        # open a transaction
        self.beginTransaction = True
        return

    def rollback(self):
        if not self.transactions:
            print "> NO TRANSACTION"
        else:
            del self.transactions[-1]

    def commit(self):
        if not self.transactions:
            print "> NO TRANSACTION"
        else:
            self.database = self.updateDB()
            del self.transactions[:]
        return


    def updateDB(self):
        if not self.transactions:# or self.hasEmptyTransaction() == False:
            return self.database

        else: # if transactions were opened:
            current_database = {}
            temp = self.transactions[:]
            temp.reverse()
            temp.append(self.database)

            for transaction in temp:
                for key in transaction:
                    if key not in current_database:
                        current_database[key] = transaction[key]

            return current_database

    def end(self):
        return sys.exit()


    def executeLine(self, args):
        print args.split("\n")[0]
        if args == "NULL":
            print "> NULL"
        else:
            args = self.parseLine(args)
            verb = args[0]
            if verb == "GET":
                self.get(args[1].split("\n")[0])

            elif verb == "SET":
                self.set(args[1], int(args[2]))

            elif verb == "UNSET":
                self.unset(args[1])

            elif verb == "NUMEQUALTO":
                self.numEqualTo(int(args[1]))

            elif verb == "BEGIN\n":
                self.begin()
            elif verb == "COMMIT\n":
                self.commit()

            elif verb == "ROLLBACK\n":
                self.rollback()

            elif verb == "END\n":
                self.end()


    def getCommandLine(self):
        part1Test = "/Users/asoong/desktop/testTransaction3"
        with open(part1Test) as file:
            for line in file:
                self.whole_line = line
                self.executeLine(line)

    def parseLine(self, args):
        return args.split(" ")

if __name__ == "__main__":
    db = database()
    db.getCommandLine()
