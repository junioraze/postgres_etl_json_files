import create_tables, etl

def execute():
    """
    Execute the complete ETL process.
    Create database tables.
    Insert json files into created tables.
    """
    create_tables.main()
    print('All tables are created!')
    print('-'*70)
    etl.main()
    print('-'*70)
    print('OMG is done!')
          
if __name__ == '__main__':
    execute()