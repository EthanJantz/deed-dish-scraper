from typing import Optional, List
from sqlalchemy import ForeignKey, String, CheckConstraint, UniqueConstraint
from sqlalchemy.types import Date, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class PinFormatMixin:
    """
    Mixin to add a check constraint ensuring the 'pin' column is
    exactly 14 digits long.
    """

    __table_args__ = (
        CheckConstraint("pin SIMILAR TO '[0-9]{14}'", name="ck_pin_format"),
    )


class Document(PinFormatMixin, Base):
    """
    The Document class defines the documents table. Documents are individual legal documents
    collected and hosted by the Cook County Recorder.
    """

    __tablename__ = "documents"

    entities: Mapped[List["Entity"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )
    pins: Mapped[List["Pin"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )
    prior_docs: Mapped[List["PriorDoc"]] = relationship(
        back_populates="document",
        foreign_keys="PriorDoc.doc_num",
        cascade="all, delete-orphan",
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    doc_num: Mapped[str] = mapped_column(String(255))
    pin: Mapped[str] = mapped_column(String(14), index=True)
    date_executed: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    date_recorded: Mapped[Date] = mapped_column(Date)
    num_pages: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    doc_type: Mapped[str] = mapped_column(String(50))
    consideration_amount: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True
    )
    pdf_url: Mapped[str] = mapped_column(String(2048))

    __table_args__ = UniqueConstraint("doc_num", name="uix_doc_num")

    def __repr__(self) -> str:
        return f"Document(doc_num={self.doc_num!r}, pin={self.pin!r}, date_executed={self.date_executed!r}, \
        date_recorded={self.date_recorded!r}, num_pages={self.num_pages!r}, address={self.address!r}, \
        doc_type={self.doc_type!r}, consideration_amount={self.consideration_amount!r}, pdf_url={self.pdf_url!r})"


class Entity(PinFormatMixin, Base):
    """
    The Entity class defines the entities table. Entities are businesses or individuals
    identified as grantors or grantees on a document.
    """

    __tablename__ = "entities"

    document: Mapped["Document"] = relationship(back_populates="entities")

    id: Mapped[int] = mapped_column(primary_key=True)
    doc_num: Mapped[str] = mapped_column(ForeignKey("documents.doc_num"))
    pin: Mapped[str] = mapped_column(String(14), index=True)
    entity_name: Mapped[str] = mapped_column(String(255), index=True)
    entity_status: Mapped[str] = mapped_column(String(20))
    trust_number: Mapped[Optional[str]] = mapped_column(String(50))

    __table_args__ = (
        UniqueConstraint("doc_num", "entity_name", "entity_status"),
        CheckConstraint(entity_status.in_(["grantor", "grantee"])),
    )

    def __repr__(self) -> str:
        return f"Entity(id={self.id!r}, doc_num={self.doc_num!r}, pin={self.pin!r}, entity_name={self.entity_name!r} \
        entity_status={self.entity_status!r}, trust_number={self.trust_number!r})"


class Pin(PinFormatMixin, Base):
    """
    The Pin class defines the pins table. This table collects
    relationships between pins as found on the Recorder's document
    page.
    """

    __tablename__ = "pins"

    document: Mapped["Document"] = relationship(back_populates="pins")

    id: Mapped[int] = mapped_column(primary_key=True)
    pin: Mapped[str] = mapped_column(String(14), index=True)
    doc_num: Mapped[str] = mapped_column(ForeignKey("documents.doc_num"))
    related_pin: Mapped[str] = mapped_column(String(14), index=True)

    __table_args__ = (
        CheckConstraint(
            "related_pin SIMILAR TO '[0-9]{14}'", name="ck_related_pin_format"
        ),
        UniqueConstraint("doc_num", "pin", "related_pin"),
    )

    def __repr__(self) -> str:
        return f"Pin(id={self.id!r}, pin={self.pin!r}, doc_num={self.doc_num!r}, related_pin={self.related_pin!r})"


class PriorDoc(Base):
    """
    The PriorDoc class defines the doc_relations table. The Cook County Recorder
    provides information about relationships between different documents, calling them prior documents.
    """

    __tablename__ = "prior_docs"

    document: Mapped["Document"] = relationship(
        back_populates="prior_docs", foreign_keys="PriorDoc.doc_num"
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    doc_num: Mapped[str] = mapped_column(ForeignKey("documents.doc_num"))
    prior_doc_num: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    __table_args__ = UniqueConstraint("doc_num", "prior_doc_num")

    def __repr__(self) -> str:
        return f"PriorDoc(id={self.id!r}, doc_num={self.doc_num!r}, prior_doc_num={self.prior_doc_num!r})"
