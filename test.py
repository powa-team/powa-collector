import psycopg2


def main():
    try:
        print("test")
        conn = psycopg2.connect("host=localhost")

        cur = conn.cursor()
        cur.execute("LOAD pouet")
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print("error: %s" % (e))

if (__name__ == "__main__"):
    main()
