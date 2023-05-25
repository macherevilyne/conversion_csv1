# Import moduls
import qrcode
import base64
from io import BytesIO



'''This function generates a qr code'''



def generate_qr_code(data):
    # qr class parameters
    qr = qrcode.QRCode(
        version=1,
        box_size=7,
        border=5
    )
    qr.add_data(data)

    # creating a qr code
    qr.make(fit=True)

    # creating a qr code image and its parameters

    img = qr.make_image(fill_color='black', back_color='white')

    buf = BytesIO()
    img.save(buf,)
    image_bytes = buf.getvalue()
    base64_bytes = base64.b64encode(image_bytes)
    base64_string = base64_bytes.decode('utf-8')

    return base64_string