from typing import Optional
from sqlalchemy import ForeignKey, String
from sqlalchemy.types import Date, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Document(Base):
    """
    The Document class defines the documents table. Documents are individual legal documents
    collected and hosted by the Cook County Recorder.
    """

    __tablename__ = "documents"

    doc_num: Mapped[str] = mapped_column(primary_key=True)
    pin: Mapped[str] = mapped_column(String(14))
    date_executed: Mapped[Optional[Date]] = mapped_column(Date)
    date_recorded: Mapped[Date] = mapped_column(Date)
    num_pages: Mapped[int] = mapped_column(Integer)
    address: Mapped[Optional[str]] = mapped_column(String(255))
    doc_type: Mapped[str] = mapped_column(String(50))
    consideration_amount: Mapped[Optional[str]] = mapped_column(String(50))
    pdf_url: Mapped[str] = mapped_column(String(2048))

    def __repr__(self) -> str:
        return f"Document(doc_num={self.doc_num!r}, pin={self.pin!r}, date_executed={self.date_executed!r}, \
        date_recorded={self.date_recorded!r}, num_pages={self.num_pages!r}, address={self.address!r}, \
        doc_type={self.doc_type!r}, consideration_amount={self.consideration_amount!r}, pdf_url={self.pdf_url!r})"


class Entity(Base):
    """
    The Entity class defines the entities table. Entities are businesses or individuals
    identified as grantors or grantees on a document.
    """

    __tablename__ = "entities"

    id: Mapped[int] = mapped_column(primary_key=True)
    doc_num: Mapped[str] = mapped_column(ForeignKey("documents.doc_num"))
    pin: Mapped[str] = mapped_column(String(14))
    entity_name: Mapped[str] = mapped_column(String(255))
    entity_status: Mapped[str] = mapped_column(String(20))
    trust_number: Mapped[Optional[str]] = mapped_column(String(50))

    def __repr__(self) -> str:
        return f"Entity(id={self.id!r}, doc_num={self.doc_num!r}, pin={self.pin!r}, entity_name={self.entity_name!r} \
        entity_status={self.entity_status!r}, trust_number={self.trust_number!r})"


class Pin(Base):
    """
    The Pin class defines the pins table. This table collects
    relationships between pins as found on the Recorder's document
    page.
    """

    __tablename__ = "pins"

    id: Mapped[int] = mapped_column(primary_key=True)
    pin: Mapped[str] = mapped_column(String(14))
    doc_num: Mapped[str] = mapped_column(ForeignKey("documents.doc_num"))
    related_pin: Mapped[str] = mapped_column(String(14))

    def __repr__(self) -> str:
        return f"Pin(id={self.id!r}, pin={self.pin!r}, doc_num={self.doc_num!r}, related_pin={self.related_pin!r})"


class PriorDoc(Base):
    """
    The PriorDoc class defines the doc_relations table. The Cook County Recorder
    provides information about relationships between different documents, calling them prior documents.
    """

    __tablename__ = "prior_docs"

    id: Mapped[int] = mapped_column(primary_key=True)
    doc_num: Mapped[str] = mapped_column(ForeignKey("documents.doc_num"))
    prior_doc_num: Mapped[str] = mapped_column(ForeignKey("documents.doc_num"))

    def __repr__(self) -> str:
        return f"PriorDoc(id={self.id!r}, doc_num={self.doc_num!r}, prior_doc_num={self.prior_doc_num!r})"
