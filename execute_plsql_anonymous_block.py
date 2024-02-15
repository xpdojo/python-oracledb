import oracledb

DB_USERNAME = ''
DB_PASSWORD = ''
DB_DSN = ''


def connect_oracle() -> oracledb.Connection:
    connection = oracledb.connect(
        user=DB_USERNAME,
        password=DB_PASSWORD,
        dsn=DB_DSN,
    )
    print("oracle.dbms.version={0}".format(connection.version))
    return connection


def main():
    print("Connecting to oracle...")
    connection = connect_oracle()

    with connection.cursor() as cursor:
        print("Call...")

        # https://python-oracledb.readthedocs.io/en/latest/user_guide/plsql_execution.html#using-dbms-output
        # enable DBMS_OUTPUT
        cursor.callproc("dbms_output.enable")

        cursor.execute("""
        DECLARE
            job_number NUMBER;
        BEGIN
            dbms_output.put_line('[' || SYSTIMESTAMP || '] BROKEN');

            SELECT 1 INTO job_number FROM dual;
            dbms_output.put_line('job: ' || job_number);
            -- dbms_job.broken(job_number, TRUE);

            dbms_output.put_line('[' || SYSTIMESTAMP || '] END BROKEN');

            COMMIT;
        END;
        """)
        # for info in cursor:
        #     print("Error at line {} position {}:\n{}".format(*info))

        # tune this size for your application
        chunk_size = 100
        # create variables to hold the output
        lines_var = cursor.arrayvar(str, chunk_size)
        num_lines_var = cursor.var(int)
        num_lines_var.setvalue(0, chunk_size)

        # fetch the text that was added by PL/SQL
        while True:
            cursor.callproc("dbms_output.get_lines", (lines_var, num_lines_var))
            num_lines = num_lines_var.getvalue()
            lines = lines_var.getvalue()[:num_lines]
            for line in lines:
                print(line or "")
            if num_lines < chunk_size:
                break

        print("Query user_jobs...")
        query = """
        SELECT job,
               what,
               broken,
               last_date,
               -- next_date, /*DPY-3022: named time zones are not supported in thin mode*/
               to_char(next_date, 'YYYY-MM-DD HH24:MI:SS') AS next_date,
               interval
        FROM user_jobs
        WHERE broken = 'N'
        -- AND what IN (
        --                'PROC_MY_JOB;'
        --     )
        --   AND 
        order by total_time, job
        """
        cursor.execute(statement=query)
        columns = [col[0] for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))
        for row in cursor:
            print(row)


if __name__ == '__main__':
    main()
    print("Done.")
