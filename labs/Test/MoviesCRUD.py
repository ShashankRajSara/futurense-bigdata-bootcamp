import mysql.connector

try:
    conn = mysql.connector.connect(host='localhost',port=3308,
                                         database='movies',
                                         user='root',
                                         password='password')
except Exception as e:
    print(e)

while True:
    usrOption = int(input("Select Any Option: \n 1. Add a Movie\n 2. View Movie Details\n 3. Update a Movie \n 4. Delete a Movie\n 5.List all Movies\n"))

    if (usrOption == 1):
        MovieId=(input("Enter Movie ID: "))
        title = input("Enter Title: ")
        year = int(input("Year of Release: "))
        genre = int(input("Genre: "))

        try:
            mycursor=conn.cursor()  
            mycursor.execute("INSERT INTO movies VALUES ( {0},'{1}',{2},{3})".format(MovieId,title,year,genre))  

        except Exception as e:
            print(e)
            conn.close()

        break
    elif (usrOption == 5):
        try:
            mycursor=conn.cursor()  
            mycursor.execute("SELECT * FROM movies")  
            result=mycursor.fetchall()
            for i in result:
                print(i)
        
        except Exception as e:
            print(e)
            conn.close()

        break
    elif (usrOption == 3):
        pass



    elif (usrOption == 4):
        MovieId=(input("Enter Movie ID: "))

        try:
            mycursor=conn.cursor()  
            mycursor.execute("DELETE FROM movies WHERE movieId={}".format(MovieId))  
        
        except Exception as e:
            print(e)
            conn.close()
        break
    else:
        print("Please Enter correct option!!")

