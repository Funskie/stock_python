import datetime

import loguru
import sqlalchemy
import sqlalchemy.ext.automap
import sqlalchemy.orm
import sqlalchemy.schema

def insert_data():
    username = 'root'     # 資料庫帳號
    password = 'root'     # 資料庫密碼
    host = '127.0.0.1'    # 資料庫位址
    port = '3306'         # 資料庫埠號
    database = 'test'   # 資料庫名稱
    # 建立連線引擎
    engine = sqlalchemy.create_engine(
        f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
    )
    # 取得資料庫元資料
    metadata = sqlalchemy.schema.MetaData(engine)
    # 產生自動對應參照
    automap = sqlalchemy.ext.automap.automap_base()
    automap.prepare(engine, reflect=True)
    # 準備 ORM 連線
    session = sqlalchemy.orm.Session(engine)

    # 載入 stocks 資料表資訊
    sqlalchemy.Table('stocks', metadata, autoload=True)
    # 取出對應 stocks 資料表的類別
    Stock = automap.classes['stocks']
    # try:
    #     # 執行原生 SQL 命令清空資料庫
    #     session.execute('TRUNCATE TABLE stocks')
    #     # 送出執行命令
    #     session.commit()
    # except Exception as e:
    #     # 發生例外錯誤，還原交易
    #     session.rollback()
    #     loguru.logger.error('清空資料失敗')

    # 進入交易模式
    try:
        # 建立第一筆資料
        stock = Stock()
        stock.code = '1101'
        stock.name = '台泥'
        session.add(stock)

        # 建立第二筆資料
        stock = Stock()
        stock.code = '1102'
        stock.name = '亞泥'
        session.add(stock)

        # 建立第三筆資料
        stock = Stock()
        stock.code = '1103'
        stock.name = '嘉泥'
        session.add(stock)

        # 建立第四筆資料
        stock = Stock()
        stock.code = '1201'
        stock.name = '味全'
        session.add(stock)

        # 寫入資料庫
        session.commit()
    except Exception as e:
        # 發生例外錯誤，還原交易
        session.rollback()
        loguru.logger.error('新增資料失敗')
        loguru.logger.error(e)

    # 關閉連線
    session.close()

def select_data():
    username = 'root'     # 資料庫帳號
    password = 'root'     # 資料庫密碼
    host = '127.0.0.1'    # 資料庫位址
    port = '3306'         # 資料庫埠號
    database = 'test'   # 資料庫名稱
    # 建立連線引擎
    engine = sqlalchemy.create_engine(
        f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
    )
    metadata = sqlalchemy.schema.MetaData(engine)
    automap = sqlalchemy.ext.automap.automap_base()
    automap.prepare(engine, reflect=True)
    session = sqlalchemy.orm.Session(engine)

    sqlalchemy.Table('stocks', metadata, autoload=True)
    Stock = automap.classes['stocks']

    loguru.logger.info('取出資料表所有資料')
    results = session.query(Stock).all()
    for stock in results:
        loguru.logger.info(f'{stock.code} {stock.name}')

    loguru.logger.info('使用過濾條件取出資料表資料')
    results = session.query(Stock).filter(Stock.code == '1102').all()
    for stock in results:
        loguru.logger.info(f'{stock.code} {stock.name}')

    loguru.logger.info('使用多重過濾條件 OR 取出資料表資料')
    results = session.query(Stock).filter(
        sqlalchemy.or_(
            Stock.code.like('%02'),
            Stock.code.like('12%')
        )
    ).all()
    for stock in results:
        loguru.logger.info(f'{stock.code} {stock.name}')

    loguru.logger.info('使用多重過濾條件 AND 取出資料表資料')
    results = session.query(Stock).filter(
        sqlalchemy.and_(
            Stock.code.like('%2'),
            Stock.name.like('%泥')
        )
    ).all()
    for stock in results:
        loguru.logger.info(f'{stock.code} {stock.name}')

    loguru.logger.info('使用排序取出資料表資料')
    results = session.query(Stock).order_by(Stock.code.desc()).all()
    for stock in results:
        loguru.logger.info(f'{stock.code} {stock.name}')

    session.close()

def upadte_data():
    username = 'root'     # 資料庫帳號
    password = 'root'     # 資料庫密碼
    host = '127.0.0.1'    # 資料庫位址
    port = '3306'         # 資料庫埠號
    database = 'test'   # 資料庫名稱
    # 建立連線引擎
    engine = sqlalchemy.create_engine(
        f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
    )
    metadata = sqlalchemy.schema.MetaData(engine)
    automap = sqlalchemy.ext.automap.automap_base()
    automap.prepare(engine, reflect=True)
    session = sqlalchemy.orm.Session(engine)

    sqlalchemy.Table('stocks', metadata, autoload=True)
    Stock = automap.classes['stocks']

    loguru.logger.info('----- 更新單筆資料 -----')
    try:
        stock = session.query(Stock).filter(
            Stock.code == '1102'
        ).one()
        stock.name = stock.name + '（水泥產業）'
        session.add(stock)

        # 寫入資料庫
        session.commit()
    except Exception as e:
        # 發生例外錯誤，還原交易
        session.rollback()
        loguru.logger.error('更新資料失敗')
        loguru.logger.error(e)

    loguru.logger.info('取出資料表所有資料')
    results = session.query(Stock).all()
    for stock in results:
        loguru.logger.info(f'{stock.code} {stock.name}')

    loguru.logger.info('----- 更新多筆資料 -----')
    try:
        session.query(Stock).filter(
            sqlalchemy.and_(
                Stock.code.like('11%'),
                Stock.code != '1102'
            )
        ).update({
            Stock.name: Stock.name + '（水泥產業）'
        }, synchronize_session=False)

        # 寫入資料庫
        session.commit()
    except Exception as e:
        # 發生例外錯誤，還原交易
        session.rollback()
        loguru.logger.error('更新資料失敗')
        loguru.logger.error(e)

    loguru.logger.info('取出資料表所有資料')
    results = session.query(Stock).all()
    for stock in results:
        loguru.logger.info(f'{stock.code} {stock.name}')

    session.close()

def delete_data():
    username = 'root'     # 資料庫帳號
    password = 'root'     # 資料庫密碼
    host = '127.0.0.1'    # 資料庫位址
    port = '3306'         # 資料庫埠號
    database = 'test'   # 資料庫名稱
    # 建立連線引擎
    engine = sqlalchemy.create_engine(
        f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
    )
    metadata = sqlalchemy.schema.MetaData(engine)
    automap = sqlalchemy.ext.automap.automap_base()
    automap.prepare(engine, reflect=True)
    session = sqlalchemy.orm.Session(engine)

    sqlalchemy.Table('stocks', metadata, autoload=True)
    Stock = automap.classes['stocks']
    
    loguru.logger.info('----- 刪除單筆資料 -----')
    try:
        stock = session.query(Stock).filter(
            Stock.code == '1102'
        ).one()
        session.delete(stock)

        # 寫入資料庫
        session.commit()
    except Exception as e:
        # 發生例外錯誤，還原交易
        session.rollback()
        loguru.logger.error('刪除資料失敗')
        loguru.logger.error(e)

    loguru.logger.info('取出資料表所有資料')
    results = session.query(Stock).all()
    for stock in results:
        loguru.logger.info(f'{stock.code} {stock.name}')
    
    loguru.logger.info('----- 刪除多筆資料 -----')
    try:
        session.query(Stock).filter(
            Stock.code.like('11%')
        ).delete(synchronize_session=False)

        # 寫入資料庫
        session.commit()
    except Exception as e:
        # 發生例外錯誤，還原交易
        session.rollback()
        loguru.logger.error('刪除資料失敗')
        loguru.logger.error(e)

    loguru.logger.info('取出資料表所有資料')
    results = session.query(Stock).all()
    for stock in results:
        loguru.logger.info(f'{stock.code} {stock.name}')

    session.close()

if __name__ == '__main__':
    loguru.logger.add(
        f'{datetime.date.today():%Y%m%d}.log',
        rotation='1 day',
        retention='7 days',
        level='DEBUG'
    )
    # insert_data()
    # select_data()
    # upadte_data()
    delete_data()