import re
import os
from gradio_client import Client, handle_file

class TransactionProcessor:
    def __init__(self, hf_space_url="jluptm/dfinasigleh-ocr"):
        self.hf_space_url = hf_space_url
        self.client = Client(hf_space_url)

    def extract_text(self, image_path):
        try:
            result = self.client.predict(
                handle_file(image_path),
                api_name="/predict"
            )
            return result.get("text", ""), result.get("success", False)
        except Exception as e:
            print(f"Error OCR: {e}")
            return str(e), False

    def parse_data(self, text, success):
        data = {
            "fecha": "",
            "monto": "",
            "referencia": "",
            "success": success
        }

        if not success:
            return data
            
        text_lower = text.lower()

        # 1. Fecha: DD/MM/YYYY o DD-MM-YYYY o DD/MM/YY
        date_pattern = r"(\d{2}[/-]\d{2}[/-]\d{2,4})"
        date_match = re.search(date_pattern, text)
        if date_match:
            raw_date = date_match.group(1).replace("-", "/")
            # Intentar normalizar a YYYY-MM-DD para el input tipo date
            try:
                parts = raw_date.split("/")
                if len(parts[2]) == 2: parts[2] = "20" + parts[2]
                data["fecha"] = f"{parts[2]}-{parts[1]}-{parts[0]}"
            except:
                data["fecha"] = raw_date

        # 2. Monto
        amount_pattern = r"(?:Monto|Bs\.?|BsS?\.?|BOLIVARES)\s?[:\.]?\s?([\d\.,]{3,})"
        amount_match = re.search(amount_pattern, text, re.IGNORECASE)
        if not amount_match:
            amount_pattern_suffix = r"([\d\.,]{3,})\s?(?:Bs\.?|BsS?\.?)"
            amount_match = re.search(amount_pattern_suffix, text, re.IGNORECASE)

        if amount_match:
            raw_val = amount_match.group(1).strip()
            # Normalización básica
            if "," in raw_val and "." in raw_val:
                if raw_val.rfind(",") > raw_val.rfind("."):
                    raw_val = raw_val.replace(".", "").replace(",", ".")
                else: 
                    raw_val = raw_val.replace(",", "")
            elif "," in raw_val:
                if len(raw_val.split(",")[-1]) == 2:
                    raw_val = raw_val.replace(",", ".")
                else:
                    raw_val = raw_val.replace(",", "")
            elif raw_val.count(".") > 1:
                 raw_val = raw_val.replace(".", "")
            
            data["monto"] = raw_val

        # 3. Referencia
        ref_pattern = r"(?:Ref|Operación|Referencia|Nro)[:\.\s]*(\d{6,14})"
        ref_match = re.search(ref_pattern, text, re.IGNORECASE)
        if ref_match:
            data["referencia"] = ref_match.group(1)
        else:
            ref_matches = re.findall(r"\b(\d{8,14})\b", text)
            if ref_matches:
                data["referencia"] = ref_matches[0]

        return data
