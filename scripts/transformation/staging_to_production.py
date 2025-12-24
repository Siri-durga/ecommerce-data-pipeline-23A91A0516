def normalize_text(val):
    return val.strip() if val else None

def normalize_email(email):
    return email.lower().strip() if email else None

def normalize_phone(phone):
    return ''.join(filter(str.isdigit, phone)) if phone else None

def title_case(val):
    return val.title() if val else None
