from aiomysql import connect as async_connect
from asyncio import get_event_loop


class MySQLLogger:
    def __init__(
            self,
            user,
            password,
            host="localhost",
            port=3306,
            db="logsDB",
            table="logs"
        ):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db = db
        self.table = table.replace(' ', '_')
        self.__createTable()

    async def __execute(self, loop, query, args=None):
        conn = await async_connect(
            user = self.user,
            password = self.password,
            host = self.host,
            port = self.port,
            db = self.db,
            loop = loop
        )
        async with conn.cursor() as cur:
            if args:
                await cur.execute(query, args)
            else:
                await cur.execute(query)
        await conn.commit()
        conn.close()

    def __asyncRun(self, query, args=None):
        loop = get_event_loop()
        loop.run_until_complete(self.__execute(loop, query, args))

    def __createTable(self):
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.db}.{self.table} (
            log_id INT AUTO_INCREMENT PRIMARY KEY,
            level VARCHAR(10),
            message TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.__asyncRun(query)

        query = f"""
        CREATE INDEX idx_level ON {self.db}.{self.table}(level);
        """
        self.__asyncRun(query)
        # loop = get_event_loop()
        # loop.run_until_complete(self.__execute(loop, query))

    def __insert(self, message, level):
        query = f'''
        INSERT INTO {self.db}.{self.table} (level, message)
        VALUES (%s, %s);
        '''
        args = (level, message)
        self.__asyncRun(query, args)
        # loop = get_event_loop()
        # loop.run_until_complete(self.__execute(loop, query, args))

    def info(self, message):
        self.__insert(message, 'info')

    def debug(self, message):
        self.__insert(message, 'debug')

    def error(self, message):
        self.__insert(message, 'error')

    def warning(self, message):
        self.__insert(message, 'warning')
