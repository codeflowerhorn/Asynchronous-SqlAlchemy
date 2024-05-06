import asyncio
from sqlalchemy import Column, Integer, String, select, update 
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker  

from prettytable import PrettyTable

Base = declarative_base() 
class Book(Base):
    __tablename__ = "books"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    author = Column(String)
    genre = Column(String)  

class BookRepository:
    def __init__(self):  
        self.semaphore = asyncio.Semaphore(50)  

    async def create_table(self):
        self.engine = create_async_engine('sqlite+aiosqlite:///books.db', echo=False)
        self.session = async_sessionmaker(self.engine, expire_on_commit=False)
  
        async with self.engine.begin() as engine:  
            await engine.run_sync(Base.metadata.create_all)    
     
    async def create_book(self, title: str, author: str, genre: str):
        async with self.semaphore:
            try:
                book = Book(title=title, author=author, genre=genre)
                async with self.session.begin() as session:
                    session.add(book)
                return True
            except:
                async with self.session.begin() as session:
                    await session.rollback()
                return False
            
    async def get_books(self):
        async with self.semaphore:
            async with self.session.begin() as session:
                query = select(Book)
                result = await session.stream(query)
                books = await result.scalars().all()
            
            return books

    async def get_book_by_id(self, id: int):
        async with self.semaphore:
            async with self.session.begin() as session:
                query = select(Book).where(Book.id == id)
                result = await session.stream(query)
                book = await result.scalars().one_or_none()
            
            return book
    
    async def update_book(self, id: int, **fields): 
        async with self.semaphore:
            try:
                async with self.session.begin() as session:
                    query = update(Book).where(Book.id == id).values(fields)
                    await session.execute(query)
                return True
            except:
                async with self.session.begin() as session:
                    await session.rollback()
                return False

    async def delete_book(self, book: Book):
        async with self.semaphore:
            async with self.session.begin() as session:
                await session.delete(book)

            return True
    
    async def close(self):
        await self.engine.dispose()

async def main():
    book_repository = BookRepository() # create repository instance
    await book_repository.create_table() # create the table
    # Insert book
    await book_repository.create_book("The Hobbit", "J,R,R Tolkien", "Fantasy")
    await book_repository.create_book("1987", "George Orwell", "Dystopian Fiction")
    await book_repository.create_book("To Kill a Mockingbird", "Harper Lee", "Southern Gothic, Bildungsroman")
    
    # Get all books
    table = PrettyTable(["id", "title", "author", "genre"])
    books = await book_repository.get_books()  
    for book in books: 
        table.add_row([book.id, book.title, book.author, book.genre]) 
    print(table)
    table.clear_rows()
   

    # Get book by id
    book = await book_repository.get_book_by_id(1)
    table.add_row([book.id, book.title, book.author, book.genre]) 
    print(table)
    table.clear_rows()

    # Update a book
    fields = { 
        "title": "I am title",
        "author": "I am author",
        "genre": "I am genre"
    }
    await book_repository.update_book(book.id, **fields)
    books = await book_repository.get_books()  
    for book in books:
        table.add_row([book.id, book.title, book.author, book.genre]) 
    print(table)
    table.clear_rows()

    # Delete a book
    book = await book_repository.get_book_by_id(1)
    await book_repository.delete_book(book)
    books = await book_repository.get_books()  
    for book in books:
        table.add_row([book.id, book.title, book.author, book.genre]) 
    print(table)

    await book_repository.close()
 
if __name__ == "__main__":
    asyncio.run(main())

    
