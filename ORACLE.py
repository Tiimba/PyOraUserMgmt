import cx_Oracle

class Oracle:
    def __init__(self, login_user, password, dsn):
        #dsn = dsn_pretty(dsn)
        try:
            self.conn = None
            self.cursor = None
            self.dsn = dsn
            splitdsn = dsn.split("/")
            dsnStr = cx_Oracle.makedsn(splitdsn[0],"1521", splitdsn[1])
            self.conn = cx_Oracle.connect(login_user, password, dsn=dsnStr)            
            self.cursor = self.conn.cursor()
        except cx_Oracle.Error as error:
            try:
                dsnStr = cx_Oracle.makedsn(splitdsn[0],"1521", service_name=splitdsn[1])
                self.conn = cx_Oracle.connect(login_user, password, dsn=dsnStr)            
                self.cursor = self.conn.cursor()
            except cx_Oracle.Error as error:
                erro, = error.args
                if erro.code == 1017:
                    print(f'Seu usuario ou Senha esta incorreto no banco de dados {dsn}')
                    with open('errors.txt','a') as f:
                        f.write(f'Seu usuario ou Senha esta incorreto no banco de dados {dsn} \n')
                elif erro.code == 12514:    
                    print("Banco de dados " + dsn + " não encontrado, verificar o SID (ou service name) e host do mesmo." + "\n")
                    with open('errors.txt','a') as f:
                        f.write(f'Banco de dados nao encontrado: {dsn} \n')
                else:
                    with open('errors.txt','a') as f:
                        f.write(dsn)                
                    print("Banco: " + self.dsn + " "+str(error) + "\n")


        except Exception as error:
            with open('errors.txt','a') as f:
                f.write(dsn)                
            print("Banco: " + self.dsn + " "+str(error) + "\n")

    def create_user(self, user, passwd, randpass,profile, tablespace, grants):
        try:
            if self.cursor:
                if len(passwd) >= 25:
                    query = "CREATE USER \"" + user.upper() + "\" IDENTIFIED BY VALUES\'" + passwd + "\' PROFILE \"" + profile.upper() + "\" DEFAULT TABLESPACE \"" + tablespace.upper() + "\""
                    print("INFO: Foi utilizado uma Hash como senha.")
                else:
                    query = "CREATE USER \"" + user.upper() + "\" IDENTIFIED BY \"" + passwd + "\" PROFILE \"" + profile.upper() + "\" DEFAULT TABLESPACE \"" + tablespace.upper() + "\""
                #S:27833FA84606285AAFCFC56DDB4F978C2824D8F10D503A242C22CDB443DA
                self.cursor.execute(query)
                print("Usuário " + user + " criado no Banco de dados "+ self.dsn + "\n")    
                
                self.grant_permissions(user,grants)
                if randpass is True:
                    print("A senha de acesso do usuário " + user + " nos bancos de dados é " + passwd + "\n")

        except cx_Oracle.Error as error:
            erro, = error.args
            if erro.code == 1920:
                print("Usuário " + user + " já está criado no banco de dados " + self.dsn + "\n" )
                with open('errors.txt','a') as f:
                    f.write(f'Usuario {user} ja esta criado no banco de dados: {self.dsn} \n')
            elif erro.code == 2380:
                print(f'Profile {profile} nao existe no banco de dados: {self.dsn} \n ' )
                with open('errors.txt','a') as f:
                    f.write(f'Profile {profile} nao existe no banco de dados: {self.dsn} \n')
            elif erro.code == 959:
                print(f'Tablespace {tablespace} nao existe no banco de dados: {self.dsn} \n ' )
                with open('errors.txt','a') as f:
                    f.write(f'Tablespace {tablespace} nao existe no banco de dados: {self.dsn} \n')
            else:                    
                print("Banco: "+ self.dsn + " usuario: "+ user +" " +str(error) + "\n")

        except Exception as error:
            print("Banco: " + self.dsn + " "+str(error))
        
    def reset_user(self, user, passwd,randpass):
        try:
            if self.cursor:
                if len(passwd) >= 25:
                    query = "ALTER USER \"" + user.upper() + "\" IDENTIFIED BY VALUES\'" + passwd + "\'"
                    print("INFO: Foi utilizado uma Hash como senha.")
                else:
                    query = "ALTER USER \""+user.upper() + "\" IDENTIFIED BY \"" + passwd + "\""
                self.cursor.execute(query)
                print("Usuário " + user + " teve sua senha resetada no banco de dados "+ self.dsn + "\n")
                if randpass is True:
                    print("A senha de acesso do usuário " + user + " nos bancos de dados é " + passwd + "\n")
        except cx_Oracle.Error as error:
            erro, = error.args
            if erro.code == 1918:
                print("Usuário " + user + " não existe banco de dados " + self.dsn + "\n" )
                with open('errors.txt','a') as f:
                    f.write(f'Usuario {user} nao existe banco de dados: {self.dsn} \n')
            else:                    
                print("Banco: "+ self.dsn + " usuario: "+ user +" " +str(error) + "\n")

        except Exception as error:
            print("Banco: " + self.dsn + " "+str(error) + "\n")

    def lock_user(self, user):
        try:
            if self.cursor:
                self.cursor.execute("ALTER USER \"" + user.upper() + "\" ACCOUNT LOCK")
                print("Usuário " + user + " bloqueado no banco de dados "+ self.dsn + "\n")
        except cx_Oracle.Error as error:
            erro, = error.args
            if erro.code == 1918:
                print("Usuário " + user + " não existe banco de dados " + self.dsn + "\n" )
                with open('errors.txt','a') as f:
                    f.write(f'Usuario {user} nao existe banco de dados: {self.dsn} \n')
            else:                    
                print("Banco: "+ self.dsn + " usuario: "+ user +" " +str(error) + "\n")
        except Exception as error:
            print("Banco: " + self.dsn + " "+str(error) + "\n")

    def unlock_user(self, user):
        try:
            if self.cursor:
                self.cursor.execute("ALTER USER \"" + user.upper() + "\" ACCOUNT UNLOCK")
                print("Usuário " + user + " desbloqueado no Banco de dados "+ self.dsn + "\n")
        except cx_Oracle.Error as error:
            erro, = error.args
            if erro.code == 1918:
                print("Usuário " + user + " não existe banco de dados " + self.dsn + "\n" )
                with open('errors.txt','a') as f:
                    f.write(f'Usuario {user} nao existe banco de dados: {self.dsn} \n')
            else:                    
                print("Banco: "+ self.dsn + " usuario: "+ user +" " +str(error) + "\n")
        except Exception as error:
            print("Banco: " + self.dsn + " "+str(error) + "\n")

    def drop_user(self, user):
        try:
            if self.cursor:
                self.cursor.execute("DROP USER \"" + user.upper() + "\"")
                print("Usuário " + user + " dropado no Banco de dados "+ self.dsn + "\n")
        except cx_Oracle.Error as error:
            erro, = error.args
            if erro.code == 1918:
                print("Usuário " + user + " não existe banco de dados " + self.dsn + "\n" )
                with open('errors.txt','a') as f:
                    f.write(f'Usuario {user} nao existe banco de dados: {self.dsn} \n')
            else:                    
                print("Banco: "+ self.dsn + " usuario: "+ user +" " +str(error) + "\n")
        except Exception as error:
            print("Banco: " + self.dsn + " "+str(error) + "\n\n")

    def grant_permissions(self,user,grants):
        for grant in grants:
            try:
                if self.cursor:
                    if "ADMIN" in grant:
                        split_grant = grant.upper().split(' WITH')
                        grant = split_grant[0]
                        query = "GRANT " + grant.upper() + " TO \"" + user.upper() + "\" WITH ADMIN OPTION"
                        self.cursor.execute(query)
                        print("Permissao "+ grant.upper() + " WITH ADMIN OPTION dada ao usuario " + user + " no Banco de dados " + self.dsn + "\n")
                    else:
                        query = "GRANT " + grant.upper() + " TO \"" + user.upper() + "\""
                        self.cursor.execute(query)
                        print("Permissao "+ grant.upper() + " dada ao usuario " + user + " no Banco de dados " + self.dsn + "\n")
            except cx_Oracle.Error as error:
                erro, = error.args
                if erro.code == 1917:
                    print("Usuário " + user + " não existe no banco de dados " + self.dsn + "\n" )
                    with open('errors.txt','a') as f:
                        f.write(f'Usuario {user} nao existe banco de dados: {self.dsn} \n')

                elif erro.code == 1919:
                    print("Grant " + grant +" não existe no banco de dados "+ self.dsn + " Usuário: " + user +  "\n" )
                    with open('errors.txt','a') as f:
                        f.write(f'Grant {grant} nao existe no banco de dados {self.dsn} Usuário: {user}\n')
                
                elif erro.code == 990:
                    print("Grant " + grant +" não existe, verificar o nome da permissão. Banco de dados: "+ self.dsn + " Usuário: " + user + "\n" )
                    with open('errors.txt','a') as f:
                        f.write(f'Grant {grant} nao existe, verificar o nome da permissao. Banco de dados: {self.dsn} Usuário: {user} \n')

                else:
                    print("Banco: "+ self.dsn + " usuario: "+ user + " " + str(erro) + "\n")
            except Exception as error:                
                print("Banco: " + self.dsn + " "+str(error) + "\n")

    
    def revoke_permissions(self, user, grants):
        for grant in grants:
            try:
                if self.cursor:
                    query = "REVOKE " + grant.upper() + " FROM \"" + user.upper() + "\""
                    print("Permissao "+ grant.upper() + " removida ao usuario " + user + " no Banco de dados " + self.dsn + "\n")
                    self.cursor.execute(query)
            except cx_Oracle.Error as error:
                erro, = error.args
                if erro.code == 1917:
                    print("Usuário " + user + " não existe no banco de dados " + self.dsn + "\n" )
                    with open('errors.txt','a') as f:
                        f.write(f'Grant {grant} nao existe no banco de dados {self.dsn} Usuário: {user}\n')
                    
                elif erro.code == 1919:
                    print("Grant " + grant +" não existe no banco de dados "+ self.dsn + "\n" )
                    with open('errors.txt','a') as f:
                        f.write(f'Grant {grant} nao existe no banco de dados {self.dsn} Usuário: {user}\n')

                elif erro.code == 1951:
                    print("Grant " + grant +" não está atribuida ao usuário "+ user + " no banco de dados " + self.dsn + "\n" )
                    with open('errors.txt','a') as f:
                        f.write(f'Grant {grant} nao esta atribuido ao usuario {user} no banco de dados {self.dsn}\n')
                else:
                    print("Banco: "+ self.dsn + " usuario: "+ user + " " + str(erro) + "\n")
            except Exception as error:                
                print("Banco: " + self.dsn + " "+str(error) + "\n")


    def kill_connection(self):
            try:
                if self.conn:
                    self.conn.close()
            except cx_Oracle.Error as error:
                print("Banco: "+ self.dsn + " " +str(error) + "\n")
            except Exception as error:
                print("Banco: " + self.dsn + " "+str(error) + "\n")