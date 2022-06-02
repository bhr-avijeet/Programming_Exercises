from pyspark.sql import SparkSession


class User:
    def __init__(self, id, age, gender, designation, salary):
        self.id = id
        self.age = age
        self.gender = gender
        self.designation = designation
        self.salary = salary

    @staticmethod
    def parse_data_to_domain_object(row):
        fields = row.split("|")
        return User(int(fields[0]), int(fields[1]), fields[2], fields[3], float(fields[4]))


if __name__ == '__main__':

    spark = SparkSession.builder.master("local").appName("SparkDemoApp").getOrCreate()

    user_data_rdd = spark.sparkContext.textFile("../../../../dataset/u.user")
    header = user_data_rdd.first()
    user_data_without_header = user_data_rdd.filter(lambda row: row not in header)

    print("Problem Statement 01")
    user_data_domain_obj = user_data_without_header.map(lambda row: User.parse_data_to_domain_object(row))

    print("Problem Statement 02")
    for record in user_data_domain_obj.map(lambda u: str(u.id) + "," + u.designation).take(5):
        print(record)

    print("Problem Statement 03")
    rdd03 = user_data_domain_obj.filter(lambda o: o.designation == "technician").map(
        lambda o: str(o.id) + "," + str(o.age) + "," + o.designation)
    for record in rdd03.take(5):
        print(record)

    print("Problem Statement 04")
    rdd03.saveAsTextFile("d:/rdd_output_3")