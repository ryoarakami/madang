import streamlit as st
import pandas as pd
import time
import duckdb
import os


db_conn = duckdb.connect(database='madang.db')
db_conn.sql("CREATE TABLE IF NOT EXISTS Customer AS SELECT * FROM 'Customer_madang.csv'")
db_conn.sql("CREATE TABLE IF NOT EXISTS Book AS SELECT * FROM 'Book_madang.csv'")
db_conn.sql("CREATE TABLE IF NOT EXISTS Orders AS SELECT * FROM 'Orders_madang.csv'")
conn = get_db_connection()

def query(sql):
    sql_upper = sql.strip().upper()
    if sql_upper.startswith(('SELECT', 'PRAGMA', 'DESCRIBE')):
        return conn.execute(sql).df().to_dict('records')
    else:
        conn.execute(sql)
        return None

books = [None]
result = query("SELECT bookid || ',' || bookname || ',' || price AS book_info FROM Book")
for res in result:
    books.append(res['book_info'])


if 'custid' not in st.session_state:
    st.session_state.custid = None
if 'current_orders' not in st.session_state:
    st.session_state.current_orders = pd.DataFrame()


st.title("📚 마당 서점 (DuckDB Backend)")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "고객 조회", "거래 입력", "신규 가입", "전체 거래 조회", "책 재고 및 등록"
])


with tab1:
    st.header("고객 조회")
    name_input = st.text_input("고객명")

    if len(name_input) > 0:
        sql = (
            "SELECT o.orderid, c.custid, c.name, b.bookname, o.orderdate, o.saleprice "
            "FROM Customer c, Book b, Orders o "
            "WHERE c.custid = o.custid AND o.bookid = b.bookid "
            f"AND c.name = '{name_input}';"
        )

        raw_result = query(sql)
        result_df = pd.DataFrame(raw_result)

        if not result_df.empty:
            st.session_state.custid = result_df['custid'].iloc[0]
            st.session_state.current_orders = result_df
            st.dataframe(result_df)
        else:
            st.warning(f"고객명 '{name_input}'의 거래 내역이 없습니다")

with tab2:
    st.header("거래 입력")

    customer_name_tab2 = st.text_input("거래 고객명:", key="customer_name_tab2")
    books_display = [b.rsplit(',', 1)[0] for b in books if b is not None]
    select_book = st.selectbox("구매 서적:", [None] + books_display, key="tab2_book_select")
    price = st.text_input("금액", key="tab2_price_input")

    if st.button('거래 입력'):
        if len(customer_name_tab2) > 0 and select_book is not None and len(price) > 0:
            
            sql_check_customer = (
                f"SELECT custid FROM Customer WHERE name = '{customer_name_tab2}';"
            )
            cust_result = query(sql_check_customer)

            if not cust_result:
                st.error(f"고객명 '{customer_name_tab2}'을 찾을 수 없습니다. 신규 등록을 해주세요.")
            else:
                found_custid = cust_result[0]['custid']
                bookid = select_book.split(",")[0]
                
                max_order_result = query("SELECT MAX(orderid) AS max_id FROM Orders")[0]['max_id']
                orderid = (max_order_result if max_order_result else 0) + 1 
                dt = time.strftime('%Y-%m-%d', time.localtime())

                sql_insert_order = (
                    "INSERT INTO Orders (orderid, custid, bookid, saleprice, orderdate) "
                    f"VALUES ({orderid}, {found_custid}, {bookid}, {price}, '{dt}');"
                )
                query(sql_insert_order)
                st.success("거래 입력 완료.")
        else:
            st.error("고객명, 구매 서적, 금액은 필수 입력 항목입니다")

with tab3:
    st.header("신규 고객 등록")

    new_custid_input = st.text_input("고객 번호:", key="new_cust_id")
    new_name = st.text_input("신규 고객명:", key="new_cust_name")
    new_address = st.text_input("주소:", key="new_cust_address")
    new_phone = st.text_input("전화번호:", key="new_cust_phone")

    if st.button('고객 등록'):
        if len(new_custid_input) > 0 and len(new_name) > 0:
            sql_check = f"SELECT custid FROM Customer WHERE custid = {new_custid_input};"
            existing_cust = query(sql_check)

            if existing_cust:
                st.error(f"이미 존재하는 번호({new_custid_input})입니다")
            else:
                sql_insert_cust = (
                    "INSERT INTO Customer (custid, name, address, phone) VALUES ("
                    f"{new_custid_input}, '{new_name}', '{new_address}', '{new_phone}');"
                )
                query(sql_insert_cust)
                st.success(f"고객 ID {new_custid_input} 고객명 '{new_name}' 등록 완료")
        else:
            st.error("고객 번호와 고객명을 입력해주세요")

with tab4:
    st.header("전체 거래 내역")

    sql_all_orders = (
        "SELECT o.orderid, c.name AS custname, b.bookname, o.saleprice, o.orderdate "
        "FROM Orders o, Customer c, Book b "
        "WHERE o.custid = c.custid AND o.bookid = b.bookid "
        "ORDER BY o.orderid DESC;"
    )
    all_orders_df = pd.DataFrame(query(sql_all_orders))
    st.dataframe(all_orders_df)

    st.subheader("특정 거래 내역 삭제")
    orderid_to_delete = st.text_input("삭제할 거래 번호 입력:", key="delete_order_id")

    if st.button('삭제'):
        if len(orderid_to_delete) > 0:
            sql_delete_order = f"DELETE FROM Orders WHERE orderid = {orderid_to_delete};"
            query(sql_delete_order)
            st.success(f"거래 번호 {orderid_to_delete} 삭제 완료.")
        else:
            st.error("삭제할 거래 번호를 입력하세요")

with tab5:
    st.header("책 재고 및 신규 등록")

    sql_all_books = "SELECT bookid, bookname, publisher, price FROM Book ORDER BY bookid ASC;"
    all_books_df = query(sql_all_books)
    st.dataframe(all_books_df)

    new_bookid_input = st.text_input("도서 번호:", key="new_book_id")
    new_bookname = st.text_input("책 제목:", key="new_book_name")
    new_publisher = st.text_input("출판사:", key="new_book_publisher")
    new_price_input = st.text_input("가격:", key="new_book_price")

    if st.button('도서 등록'):
        if len(new_bookid_input) > 0 and len(new_bookname) > 0 and len(new_price_input) > 0:
            sql_check = f"SELECT bookid FROM Book WHERE bookid = {new_bookid_input};"
            existing_book_df = query(sql_check)

            if not existing_book_df.empty:
                st.error(f"이미 존재하는 도서 번호({new_bookid_input})입니다.")
            else:
                sql_insert_book = (
                    f"INSERT INTO Book (bookid, bookname, publisher, price) VALUES ("
                    f"{new_bookid_input}, '{new_bookname}', '{new_publisher}', {new_price_input});"
                )
                query(sql_insert_book)
                st.success(f"도서 ID {new_bookid_input}, '{new_bookname}' 등록 완료.")
        else:
            st.error("도서 번호, 제목, 가격 입력은 필수입니다.")

    st.subheader("도서 삭제")
    bookid_to_delete = st.text_input("삭제할 도서 번호 입력:", key="delete_book_id")

    if st.button('도서 삭제 실행'):
        if len(bookid_to_delete) > 0:
            sql_delete_book = f"DELETE FROM Book WHERE bookid = {bookid_to_delete};"
            query(sql_delete_book)
            st.success(f"도서 번호 {bookid_to_delete} 삭제 완료.")
        else:
            st.error("삭제할 도서 번호를 입력하세요.")
