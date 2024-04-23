import mysql.connector
from ibookdb import IBOOKDB
from queryresult import QueryResult

class BOOKDB(IBOOKDB):

    def __init__(self,user,password,host,database,port):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        self.connection = None

    def initialize(self):
        self.connection = mysql.connector.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
            port=self.port
        )

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()

    # Tablo ekleme islemleri icin yardimci fonksiyon
    def insertFunction(self, data_path, table_name, *columns):
        count_successfully_added = 0
        count_failed = 0
        with open(data_path, 'r') as file:
            for line in file:
                try:
                    # Veriyi ayır
                    data = line.strip().split('\t')

                    # Parametreden gelen verileri sorguya uygun formata getirelim.
                    placeholders = ', '.join(['%s'] * len(columns))
                    query_columns = ', '.join(columns)

                    # Sorgu
                    insert_query = f"INSERT INTO {table_name} ({query_columns}) VALUES ({placeholders})"

                    # Ekle
                    cursor = self.connection.cursor()
                    cursor.execute(insert_query, data)
                    self.connection.commit()
                    count_successfully_added += 1
                    # print(f"{data} successfully added!")
                except Exception as e:
                    count_failed += 1
                    print(f"{data} could not added: {e}")

        print(f"Count of successfully added to {table_name}: {count_successfully_added}")
        print(f"Count of insertion failed to {table_name}: {count_failed}")

        return count_successfully_added
    
    # Sorgulardan donen degerler icin yardımcı fonksiyon
    def fetch_datas(self, query, function_name):
        datas = []
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            datas = cursor.fetchall()
            print(f"Function {function_name} worked!")
        except Exception as e:
            print(f"Function {function_name} did not work: {e}")
            
        return datas


    def createTables(self):
        create_author_table_query = """
        CREATE TABLE author(
            author_id INT PRIMARY KEY,
            author_name VARCHAR(60)
        );
        """
        create_publisher_table_query = """
        CREATE TABLE publisher(
            publisher_id INT PRIMARY KEY,
            publisher_name VARCHAR(50)
        );
        """
        create_book_table_query = """
        CREATE TABLE book (
            isbn CHAR(13) PRIMARY KEY,
            book_name VARCHAR(120),
            publisher_id INT,
            first_publish_year CHAR(4),
            page_count INT,
            category VARCHAR(25),
            rating FLOAT,
            FOREIGN KEY (publisher_id) REFERENCES publisher(publisher_id) ON DELETE CASCADE
        );
        """
        create_author_of_table_query = """
        CREATE TABLE author_of (
            isbn CHAR(13),
            author_id INT,
            FOREIGN KEY (isbn) REFERENCES book(isbn) ON DELETE CASCADE,
            FOREIGN KEY (author_id) REFERENCES author(author_id) ON DELETE CASCADE
        );
        """
        create_phw1_table_query = """
        CREATE TABLE phw1 (
            isbn CHAR(13) PRIMARY KEY,
            book_name VARCHAR(120),
            rating FLOAT
        );
        """

        create_table_query_list = [create_author_table_query, create_publisher_table_query, create_book_table_query, create_author_of_table_query, create_phw1_table_query]
        successfully_created = 0
        failed = 0
        try:
            cursor = self.connection.cursor()
            for query in create_table_query_list:
                cursor.execute(query)
                successfully_created += 1
                # print("Table created!")
        except Exception as e:
            failed += 1
            print(f"Could not created table: {e}")

        print(f"Table Created: {successfully_created}")
        print(f"Table Creation Fail: {failed}")

        return successfully_created


    def dropTables(self):
        count_dropped_tables = 0
        failed = 0
        # Referans kısıtlamalarını geçici olarak devre dışı bırakma
        try:
            cursor = self.connection.cursor()
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        except Exception as e:
            print("Constraints Error:", e)

        find_tables_from_db = "SHOW TABLES"

        cursor = self.connection.cursor()
        cursor.execute(find_tables_from_db)

        # Veritabanındaki tum tablolar
        tables = cursor.fetchall()

        for table in tables:
            drop_table_query = f"DROP TABLE IF EXISTS {table[0]} CASCADE"
            try:
                cursor.execute(drop_table_query)
                count_dropped_tables += 1
                # print(f"Table '{table[0]}' dropped")
            except Exception as e:
                failed += 1
                print(f"Table '{table[0]}' could not dropped: {e}")

        print(f"Table Dropped: {count_dropped_tables}")
        print(f"Table Drop Fail: {failed}")
        
        return count_dropped_tables

    def insertAuthor(self,authors):
        return self.insertFunction("data/dump_author.txt", "author", "author_id", "author_name")
      
    def insertBook(self,books):
        return self.insertFunction("data/dump_book.txt", "book", "isbn", "book_name", "publisher_id",
                                   "first_publish_year", "page_count", "category", "rating")

    def insertPublisher(self,publishers):
        return self.insertFunction("data/dump_publisher.txt", "publisher", "publisher_id", "publisher_name")

    def insertAuthor_of(self,author_ofs):
        return self.insertFunction("data/dump_author_of.txt", "author_of", "isbn", "author_id")

    def functionQ1(self):
        query = """
        SELECT isbn, first_publish_year, page_count, publisher_name
        FROM book b, publisher p 
        WHERE b.page_count = (SELECT MAX(page_count) from book) and b.publisher_id = p.publisher_id 
        ORDER BY isbn ASC;
        """
        datas = []
        datas_temp = self.fetch_datas(query, 'Q1')
            
        for data in datas_temp:
            datas.append(QueryResult.ResultQ1(data[0], data[1], data[2], data[3]).__str__())
            
        return datas

    def functionQ2(self,author_id1, author_id2):
        query = f"""
        SELECT t.publisher_id, AVG(b.page_count) as average_page_count
        FROM (
            SELECT p.publisher_id
            FROM author_of ao, book b, publisher p
            WHERE ao.isbn = b.isbn
                AND b.publisher_id = p.publisher_id
                AND author_id = {author_id1}
            UNION
            SELECT p.publisher_id
            FROM author_of ao, book b, publisher p
            WHERE ao.isbn = b.isbn
                AND b.publisher_id = p.publisher_id
                AND author_id = {author_id2}
        ) t, book b
        WHERE b.publisher_id = t.publisher_id
        GROUP BY publisher_id
        ORDER BY publisher_id ASC;
        """
        
        datas = []
        datas_temp = self.fetch_datas(query, 'Q2')
            
        for data in datas_temp:
                datas.append(QueryResult.ResultQ2(data[0], float(data[1])).__str__())
            
        return datas
    
    
        
    def functionQ3(self,author_name):
        query = f"""
        SELECT b.book_name, b.category, b.first_publish_year
        FROM book b, author a, author_of ao
        WHERE b.first_publish_year = (
            SELECT MIN(first_publish_year)
            FROM book b, author a, author_of ao
            WHERE b.isbn = ao.isbn
            AND ao.author_id = a.author_id
            AND author_name = '{author_name}'
        )
        AND b.isbn = ao.isbn
        AND ao.author_id = a.author_id
        AND author_name = '{author_name}'
        ORDER BY b.book_name ASC, b.category ASC, b.first_publish_year ASC;
        """
        
        datas = []
        datas_temp = self.fetch_datas(query, 'Q3')
            
        for data in datas_temp:
                datas.append(QueryResult.ResultQ3(data[0], data[1], data[2]).__str__())
            
        return datas
        
    def functionQ4(self):
        query = """
        SELECT DISTINCT t.publisher_id, b.category
        FROM (
            SELECT t.publisher_id, AVG(rating) as average_rating
            FROM (
                SELECT p.publisher_id, count(*) as count_book
                FROM book b, publisher p
                WHERE p.publisher_id IN (
                    SELECT publisher_id
                    FROM publisher
                    WHERE publisher_name LIKE '_% _% _%'
                )
                AND b.publisher_id = p.publisher_id
                GROUP BY p.publisher_id
                HAVING count_book >= 3
            ) t, book b
            WHERE b.publisher_id = t.publisher_id
            GROUP BY b.publisher_id
            HAVING average_rating > 3
        )t, book b
        WHERE b.publisher_id = t.publisher_id
        ORDER BY b.publisher_id ASC, b.category ASC;
        """
        
        datas = []
        datas_temp = self.fetch_datas(query, 'Q4')
        
        for data in datas_temp:
            datas.append(QueryResult.ResultQ4(data[0], data[1]).__str__())
            
        return datas
        
    def functionQ5(self,author_id):
        query = f"""
        SELECT a.author_id, author_name
        FROM book b, author a, author_of ao
        WHERE b.publisher_id IN (
            SELECT publisher_id
            FROM book b, author_of ao
            WHERE b.isbn = ao.isbn
                AND author_id = {author_id}
            GROUP BY publisher_id
        )
            AND b.isbn = ao.isbn
            AND ao.author_id = a.author_id
        GROUP BY author_id
        ORDER BY author_id ASC;
        """
        
        datas = []
        datas_temp = self.fetch_datas(query, 'Q5')
        
        for data in datas_temp:
            datas.append(QueryResult.ResultQ5(data[0], data[1]).__str__())
            
        return datas
        
    def functionQ6(self):
        authors = """
        SELECT DISTINCT t.author_id
        FROM (
            SELECT DISTINCT publisher_id, a.author_id
            FROM book b, author a, author_of ao
            where b.isbn = ao.isbn 
                and ao.author_id = a.author_id
            ) t
        GROUP BY publisher_id
        HAVING COUNT(*) < 2;
        """
        
        temp = self.fetch_datas(authors, 'Q6 get author_ids')
        author_ids = [data[0] for data in temp]
        
        problem_author_id = []
        
        for author_id in author_ids:
            query = f"""
            SELECT COUNT(*)
            FROM book b, author a, author_of ao
            WHERE b.isbn = ao.isbn 
                AND ao.author_id = a.author_id
                AND publisher_id IN (
                    SELECT DISTINCT b.publisher_id
                    FROM book b, author a, author_of ao
                    WHERE b.isbn = ao.isbn
                        AND ao.author_id = a.author_id
                        AND a.author_id = {author_id}
                )
                AND a.author_id != {author_id}
            """
            
            count_author = self.fetch_datas(query, 'Q6')[0][0]
            if count_author == 0:
                problem_author_id.append(author_id)
                
        query = f"""
        SELECT a.author_id, b.isbn
        FROM book b, author a, author_of ao
        WHERE b.isbn = ao.isbn 
            AND ao.author_id = a.author_id
            AND a.author_id IN ({', '.join(map(str, problem_author_id))})
        ORDER BY a.author_id ASC, b.isbn ASC; 
        """
        
        datas = []
        datas_temp = self.fetch_datas(query, 'Q6')
        
        for data in datas_temp:
            datas.append(QueryResult.ResultQ6(data[0], data[1]).__str__())
            
        return datas
        
        
    def functionQ7(self,rating):
        query = f"""
        SELECT p.publisher_id, p.publisher_name
        FROM (
            SELECT publisher_id, AVG(rating) as average_rating
            FROM book
            WHERE publisher_id IN (
                SELECT b.publisher_id
                FROM book b, author a, author_of ao
                WHERE b.isbn = ao.isbn
                    AND ao.author_id = a.author_id
                    AND category = 'Roman'
                GROUP BY publisher_id
                HAVING count(*) >= 2
            )
            GROUP BY publisher_id
            HAVING average_rating > {rating}
        ) t, publisher p
        WHERE t.publisher_id = p.publisher_id;
        """
        
        datas = []
        datas_temp = self.fetch_datas(query, 'Q7')
        
        for data in datas_temp:
            datas.append(QueryResult.ResultQ7(data[0], data[1]).__str__())
            
        return datas
        
    def functionQ8(self):
        insert = """
        INSERT INTO phw1 (isbn, book_name, rating)
        SELECT isbn, book_name, rating
        FROM book
        WHERE rating = (
            SELECT DISTINCT MIN(b1.rating)
            FROM book b1, book b2
            WHERE b1.book_name = b2.book_name
                AND b1.isbn != b2.isbn
        )
            AND isbn in (
                SELECT DISTINCT b1.isbn
                FROM book b1, book b2
                WHERE b1.book_name = b2.book_name
                    AND b1.isbn != b2.isbn
            );
        """
        
        query = """
        SELECT * FROM phw1 ORDER BY isbn ASC;
        """
        
        cursor = self.connection.cursor()
        cursor.execute(insert)
        self.connection.commit()
        
        datas = []
        datas_temp = self.fetch_datas(query, 'Q8')
        
        for data in datas_temp:
            datas.append(QueryResult.ResultQ8(data[0], data[1], data[2]).__str__())
            
        return datas
        
    def functionQ9(self,keyword):
        update = f"""
        UPDATE book
        SET rating = LEAST(5, rating + 1)
        WHERE book_name LIKE '%{keyword}%'
            and rating <= 4;
        """
        
        query = """
        SELECT SUM(rating) FROM book
        """
        
        cursor = self.connection.cursor()
        cursor.execute(update)
        self.connection.commit()
        
        data = self.fetch_datas(query, 'Q9')
        
        return data[0][0]
        
    def function10(self):
        delete = """
        DELETE FROM publisher
        WHERE publisher_id IN (
            SELECT *
            FROM (
                SELECT DISTINCT p.publisher_id
                FROM publisher p
                LEFT JOIN book b ON p.publisher_id = b.publisher_id
                WHERE b.isbn IS NULL
            ) t
        );
        """
        
        query = """
        SELECT COUNT(*) FROM publisher;
        """

        try:
            cursor = self.connection.cursor()
            cursor.execute(delete)
            self.connection.commit()
            # print("deleted")
        except Exception as e:
            print(f"Exception was occured: {e}")
        
        data = self.fetch_datas(query, "Q10")
        
        return data[0][0]
