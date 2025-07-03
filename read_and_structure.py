from pydantic import BaseModel
from google import genai
from pdf2image import convert_from_path, convert_from_bytes
from pdf2image.exceptions import (
    PDFInfoNotInstalledError,
    PDFPageCountError,
    PDFSyntaxError
)
from PIL import Image
from google.genai import types
import os
import httpx
import pathlib
import re
import logging
from dotenv import load_dotenv
import tempfile
import pandas as pd
from utils import *
from transformers import AutoTokenizer, AutoProcessor, AutoModelForImageTextToText

load_dotenv()
api_key = os.environ["GOOGLE_API_KEY"]
logger = logging.getLogger(__name__)
logging.basicConfig(filename=os.path.curdir + "/reader.log", format='%(asctime)s %(message)s', encoding='utf-8', level=logging.INFO)

class Info(BaseModel):
    owner: str
    address: str
    pin: str
    brief_document_summary: str

class Reader:

  def __init__(self):
    # Set up OCR model
    model_path = "nanonets/Nanonets-OCR-s"

    self.model = AutoModelForImageTextToText.from_pretrained(
        model_path, 
        torch_dtype="auto", 
        device_map="auto"
        # attn_implementation="flash_attention_2"
    )
    self.model.eval()

    self.tokenizer = AutoTokenizer.from_pretrained(model_path)
    self.processor = AutoProcessor.from_pretrained(model_path)

  def convert_to_image(self, path):
    print('converting...')
    self.imgpath = re.sub('pdf', 'png', path)
    images_from_path = convert_from_path(path)
    images_from_path[0].save(self.imgpath)
    print('converted.')

  def run_ocr(self,max_new_tokens=4096):
    print('ocr-ing...')
    prompt = """Extract the text from the above document as if you were reading it naturally. Return the tables in html format. Return the equations in LaTeX representation. If there is an image in the document and image caption is not present, add a small description of the image inside the <img></img> tag; otherwise, add the image caption inside <img></img>. Watermarks should be wrapped in brackets. Ex: <watermark>OFFICIAL COPY</watermark>. Page numbers should be wrapped in brackets. Ex: <page_number>14</page_number> or <page_number>9/22</page_number>. Prefer using ☐ and ☑ for check boxes."""
    image = Image.open(self.imgpath)
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": [
            {"type": "image", "image": f"file://{self.imgpath}"},
            {"type": "text", "text": prompt},
        ]},
    ]
    text = self.processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = self.processor(text=[text], images=[image], padding=True, return_tensors="pt")
    inputs = inputs.to(self.model.device)
    
    output_ids = self.model.generate(**inputs, max_new_tokens=max_new_tokens, do_sample=False)
    generated_ids = [output_ids[len(input_ids):] for input_ids, output_ids in zip(inputs.input_ids, output_ids)]
    
    self.ocr_output = self.processor.batch_decode(generated_ids, skip_special_tokens=True, clean_up_tokenization_spaces=True)
    return self.ocr_output[0]
    
  def structure_data(self, ocr_output):
    prompt = "What is the property being discussed in this document and who is the owner of said property? Provide a brief summary of the document as well."
    response = client.models.generate_content(
      model="gemini-2.5-flash",
      contents=[
          types.Part.from_bytes( 
            data=ocr_output,
            mime_type='application/pdf',
          ),
          prompt
      ],
      config=types.GenerateContentConfig(
          response_mime_type="application/json",
          response_schema=list[Info],
          temperature=0
      )
    ) 
    
if __name__ == "__main__":
  r = Reader()
  path = '/home/ethan/deeds-research/data/19241250280000/0020865706.pdf'
  r.convert_to_image(path)
  r.run_ocr()