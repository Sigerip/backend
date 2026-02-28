import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

def enviar_email_boas_vindas(email_destino, token_gerado, nome):
    # Puxa as credenciais das variáveis de ambiente (nunca deixe senhas soltas no código!)
    email_remetente = os.environ.get("EMAIL_REMETENTE")
    senha_remetente = os.environ.get("SENHA_EMAIL_APP")

    # Monta a estrutura do e-mail
    msg = MIMEMultipart()
    msg['From'] = email_remetente
    msg['To'] = email_destino
    msg['Subject'] = f"Sua Chave de API - Bem-vindo {nome}!"

    # Cria o texto personalizado (usando HTML para ficar bem formatado)
    corpo_email = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
        <h2>Olá {nome}! Obrigado por se cadastrar.</h2>
        <p>Sua conta foi criada com sucesso. Abaixo está a sua chave de acesso (Token) exclusiva para utilizar a nossa API:</p>
        
        <div style="background-color: #f4f4f4; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 16px; margin: 20px 0;">
          <strong>{token_gerado}</strong>
        </div>
        
        <p><strong>Aviso importante:</strong> Guarde este token em segurança. Você precisará enviá-lo no cabeçalho (Authorization) de todas as requisições que fizer ao nosso sistema.</p>
        
        <p>Se você tiver qualquer dúvida ou encontrar algum problema técnico, não hesite em entrar em contato respondendo a este e-mail.</p>
        
        <p>Abraços,<br><strong>Equipe Observatório de Inteligência Atuarial</strong></p>
      </body>
    </html>
    """
    
    # Anexa o texto em formato HTML
    msg.attach(MIMEText(corpo_email, 'html'))

    try:
        # Configuração de conexão com o servidor do Gmail
        servidor = smtplib.SMTP('smtp.gmail.com', 587)
        servidor.starttls() # Inicia a criptografia
        servidor.login(email_remetente, senha_remetente)
        
        # Envia o e-mail e fecha a conexão
        servidor.send_message(msg)
        servidor.quit()
        return True
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")
        return False
    
def reenviar_email_token(email_destino, token_recuperado,nome):
    email_remetente = os.environ.get("EMAIL_REMETENTE")
    senha_remetente = os.environ.get("SENHA_EMAIL_APP")

    msg = MIMEMultipart()
    msg['From'] = email_remetente
    msg['To'] = email_destino
    msg['Subject'] = "Recuperação de Chave de API - Observatório de Inteligência Atuarial"

    corpo_email = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
        <h2>Olá novamente {nome}!</h2>
        <p>Notamos que você tentou se cadastrar, mas este e-mail já possui uma conta ativa em nosso sistema.</p>
        <p>Como medida de segurança e para facilitar o seu acesso, estamos reenviando a sua chave de API (Token) atual:</p>
        
        <div style="background-color: #f4f4f4; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 16px; margin: 20px 0;">
          <strong>{token_recuperado}</strong>
        </div>
        
        <p>Lembre-se de utilizar este token no cabeçalho (Authorization) das suas requisições.</p>
        <p>Se você não solicitou este reenvio, por favor, ignore esta mensagem.</p>
        
        <p>Abraços,<br><strong>Equipe Observatório de Inteligência Atuarial</strong></p>
      </body>
    </html>
    """
    
    msg.attach(MIMEText(corpo_email, 'html'))

    try:
        servidor = smtplib.SMTP('smtp.gmail.com', 587)
        servidor.starttls()
        servidor.login(email_remetente, senha_remetente)
        servidor.send_message(msg)
        servidor.quit()
        return True
    except Exception as e:
        print(f"Erro ao reenviar e-mail: {e}")
        return False