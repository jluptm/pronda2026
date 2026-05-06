import os
from PIL import Image, ImageDraw, ImageFont

def generate_certificate(nombres, apellidos, cedula, categoria):
    base_dir = "certif/imabase"
    output_dir = "certif"
    font_dir = "certif"
    
    # Map category to base image
    cat_map = {
        "Ministro Cristiano": "Ministro Cristiano 2026.png",
        "Ministro Distrital": "Ministro Distrital 2026.png",
        "Ministro Licenciado": "Ministro Licenciado 2026.png",
        "Ministro Ordenado": "Ministro Ordenado 2026.png"
    }
    
    base_file = None
    # Normalización de categoría
    norm_cat = str(categoria).strip()
    if "Cristiano" in norm_cat: base_file = cat_map["Ministro Cristiano"]
    elif "Distrital" in norm_cat: base_file = cat_map["Ministro Distrital"]
    elif "Licenciado" in norm_cat: base_file = cat_map["Ministro Licenciado"]
    elif "Ordenado" in norm_cat: base_file = cat_map["Ministro Ordenado"]
    
    if not base_file:
        return None, f"Categoría '{categoria}' no reconocida para imagen base."
        
    base_path = os.path.join(base_dir, base_file)
    if not os.path.exists(base_path):
        return None, f"No se encontró la imagen base: {base_path}"
        
    try:
        img = Image.open(base_path).convert('RGB')
        draw = ImageDraw.Draw(img)
        
        # Load fonts
        name_font_path = os.path.join(font_dir, "Sofia-Regular.ttf")
        # Use Roboto-Bold (copied from Arial Bold in previous step)
        ced_font_path = os.path.join(font_dir, "Roboto-Bold.ttf")
        
        if not os.path.exists(name_font_path) or not os.path.exists(ced_font_path):
            return None, "Faltan archivos de fuentes (.ttf) en la carpeta certif."
        
        # Coordinates (Center of yellowish boxes)
        # Name Box Center=(1294, 446)
        # Cedula Box Center=(1438, 576)
        NAME_CENTER = (1294, 446)
        CED_CENTER = (1438, 576)
        
        name_text = f"{nombres} {apellidos}"
        ced_text = str(cedula)
        
        # FIXED font sizes as requested
        NAME_FONT_SIZE = 60 
        CED_FONT_SIZE = 38
        
        name_font = ImageFont.truetype(name_font_path, NAME_FONT_SIZE)
        ced_font = ImageFont.truetype(ced_font_path, CED_FONT_SIZE)
        
        # DRAW NAME with perfect centering
        # anchor="mm" means Middle Horizontal, Middle Vertical
        # We add a slight Y offset (-5) if the script font's baseline is high
        draw.text(NAME_CENTER, name_text, font=name_font, fill=(0, 0, 0), anchor="mm")
        
        # DRAW CEDULA with perfect centering
        draw.text(CED_CENTER, ced_text, font=ced_font, fill=(0, 0, 0), anchor="mm")
        
        # Clean filename
        clean_name = f"{nombres}_{apellidos}".replace(" ", "_")
        output_name = f"{clean_name}_{cedula}.png"
        output_path = os.path.join(output_dir, output_name)
        img.save(output_path)
        return output_path, None
    except Exception as e:
        return None, str(e)
