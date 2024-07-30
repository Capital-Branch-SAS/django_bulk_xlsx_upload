from django.db import models
from django.db.models.fields.related import ForeignKey
import pyexcel
from typing import Union, Dict
import datetime
from copy import deepcopy

class DjangoBulkXLSXUpload():
    '''
    Main class for bulk upload of Django models from a
    Microsoft Excel XLSX file
    '''

    def __init__(self, rules = None, saveKwargs: Union[Dict, None] = None):
        self.file = None
        self.sheet = None
        self.records = []
        self.bulk_save = False
        # Array por modelos
        self.saved_models = {}
        self.rules: list[UploadRule] = []
        self.saveKwargs: Union[Dict, None] = None
        if rules:
            self.loadRules(rules)

    def load(self, file, column_by_row = 0, samples=0):
        '''
        Load a Excel file where:
            - column_by_row: Row with column names
            - samples: Quantity of samples to ignore by this function
                        after row of column names
        '''
        if len(self.rules) == 0:
            raise Exception("No rules are loaded, load them first")
        try:
            self.file = file
            content = file.read()
            self.sheet = pyexcel.get_sheet(file_type = "xlsx", file_content = content)
        except:
            raise Exception("Failed to load file")
        self.sheet.name_columns_by_row(column_by_row)
        records = self.sheet.to_records()
        for i, record in enumerate(records):
            # Acá viene la lógica de tipos
            # if i > column_by_row:
            values = list(record.values())
            vacios = [x == "" for x in values]
            if (vacios.count(True) == len(vacios)):
                break
            self.records.append(record)
        # Ahora vamos a iterar por reglas de modelos
        # con los datos de las reglas con sus respectivos
        # modelos prioritarios
        cont = 0;
        currentIndex = -1
        errores = []
        while cont < len(self.rules):
            # Probamos con todos las reglas con prioridad de index
            # currentIndex. Inicia el bucle con 0
            currentIndex += 1
            for i, rule in enumerate(self.rules):
                # Validar
                if rule.order == currentIndex:
                    objs, errores = rule.generateItems(self.records, self.saved_models)
                    # Almacenar todos los objetos creados de ese modelo
                    self.saved_models[rule.model] = objs
                    cont += 1
        return errores

    def loadRules(self, data, bulk_save=False):
        '''
        Acá vamos a ordenar el tema de la librería de la carga masiva.
        Así que:
            - Nombre de la clave: campo a llenarse, ejemplo: modelo.campo
            - type: Tipo de relación a crear
            - model: Si hay un modelo asociado, acá va. Ya sea de relación o anterior para asignar
                (es decir, por ejemplo los datos de contacto de un modelo posterior de la regla)
            - remoteAttribute: Atributo remoto que hay que buscar según el tipo de relación
            - value: Según el caso, asigna directamente el valor (type: fixed)
            - separator: Valor separador de relaciones múltiples
        '''
        for model in data.keys():
            # Hay que revisar por cada modelo por si hay saveKwargs
            assert(isinstance(model, models.base.ModelBase))
            reglas: Union[Dict, None] = None
            if isinstance(self.saveKwargs, dict):
                if model in self.saveKwargs.keys():
                    reglas = self.saveKwargs[model]
            values = data[model]
            print(f"Las reglas son!: {reglas} y el modelo es {model}")
            rule = UploadRule(model, saveKwargsRule = reglas)
            for keyModel in values.keys():
                # keymodel es el nombre del atributo
                # Puede ser el nombre de la columna si es string
                # O el modelo al que hace referencia de la misma
                # fila de Excel
                # Por último, puede ser una tupla para indicar una
                # relación de ManyToManyField en donde se tienen
                # los tres valores:
                # (model, columnName, attname, sep)
                #   model: Modelo a referenciar
                #   columnName: Nombre de la columna en el Excel del valor a buscar
                #   attname: Nombre del atibuto del modelo para buscar
                #   sep: Separador textual de cada campo a buscar en la relación

                # Se podría manejar una tupla de solo 3 separadores para asignaciones
                # de foreign key
                column: dict = values[keyModel]
                # isColumn = isinstance(column, str) and column != ""
                # isModel = isinstance(column, models.base.ModelBase)
                # isTuple = isinstance(column, tuple) and column != ""
                isDict = isinstance(column, dict)
                if not isDict:
                    raise Exception(
                        "Value of {0} isn't string or a valid Django Model".format(column))
                if column.get('type') == "array_hstore":
                    # Es un arreglo de hstores
                    # addMatch(self, nameCol, attribute = None, model = None, sep = None, remoteAttribute = None, localAttr = None, fields = None):
                    rule.addMatch(
                            nameCol = column.get('column'),
                            model = None,
                            attribute = column.get('parameter'),
                            fields = column.get('fields'),
                            typeMatch = 'array_hstore'
                            )
                elif column.get('type') == 'fixed':
                    rule.addMatch(
                            attribute = keyModel,
                            typeMatch = 'fixed',
                            # En value viene el modelo o dato a asignar
                            # directo
                            fixedValue = column.get('value')
                            )
                elif column.get('type') == 'manytomany':
                    rule.addMatch(
                            nameCol = column.get('column'),
                            attribute= keyModel,
                            model = column.get('model'),
                            sep = column.get('separator'),
                            remoteAttribute = column.get('remoteAttribute'),
                            typeMatch = 'manytomany'
                            )
                elif column.get('type') == 'foreign':
                    rule.addMatch(
                            attribute = keyModel,
                            nameCol = column.get('column'),
                            model = column.get('model'),
                            remoteAttribute = column.get('remoteAttribute'),
                            typeMatch = 'foreign'
                            )
                elif column.get('type') == 'simple':
                    rule.addMatch(
                            attribute = keyModel,
                            nameCol = column.get('column'),
                            typeMatch = "simple"
                            )
                else:
                    raise Exception("Falta especificar bien el tipo")
            self.rules.append(rule)
        if len(self.rules) == 1 and bulk_save:
            self.bulk_save = True
        self.defineOrder()

    def defineOrder(self):
        '''
        Definir el orden de guardado de cada modelo
        '''
        # Vamos a darle números más bajos a los que no tienen dependencias
        # de modelos
        modelsDep = [{"model": rule.model, "index": i, "relations": []}
                     for i, rule in enumerate(self.rules)]
        # Primero se hace un paneo de las relaciones
        for modelDict in modelsDep:
            fields = modelDict["model"]._meta.fields
            for relation in fields:
                if isinstance(relation, ForeignKey):
                    # relation.name es el nombre del atributo del modelo vinculado
                    # y related_model es el modelo al que apunta
                    if(relation.related_model in [x.model for x in self.rules]):
                        modelDict["relations"].append({
                            "name": relation.name,
                            "related_model": relation.related_model
                            })
        # Ahora podemos pasar a iterar la cosa
        cont = 0
        order = 0
        while cont < len(modelsDep):
            for modelDict in modelsDep:
                index = modelDict["index"]
                # La condición no es que la cantidad de relaciones sean cero,
                # sino que las relaciones con modelos cargados sea cero!
                if(len(modelDict["relations"]) == 0 and not self.rules[index]._ordered):
                    # Si no hay relaciones, de una!
                    self.rules[index].order = order
                    self.rules[index]._ordered = True
                    # Ahora, a limpiar referencias de ese modelo
                    # en otros modelos
                    for modelClean in modelsDep:
                        cleanIndexes = [i for i, x in enumerate(modelClean["relations"])
                                        if x["related_model"] == modelDict["model"]]
                        # Ahora a borrarlos
                        modelClean["relations"] = [x for i, x in enumerate(modelClean["relations"])
                                                   if i not in cleanIndexes]
                    # Se pudo, entonces a aumentar cont
                    cont += 1
            # Ya se hizo una iteración de modelos
            order += 1


class UploadRule():
    '''
    Class for rules!
    Cada regla está definida por:
        - Un modelo de Django
        - Este modelo tiene varias correspondencias de columnas
    '''
    def __init__(self, model, saveKwargsRule: Union[Dict, None] = None):
        self.model: models.Model = model
        self.matches: list[Match] = []
        self.order: int = 0
        self._ordered = False
        # objs será una lista de mapas con la estructura:
        # {index: int, obj: obj | None}
        self.objs = []
        self.saveKwargsRule: Union[Dict, None] = saveKwargsRule
        # itemsModel contendrá la lista de los items
        # del modelo junto con sus tipos
        self.itemsModel = []
        for field in model._meta.fields:
            self.itemsModel.append({
                "name": field.attname,
                "type": field.get_internal_type()
                })

    def addMatch(self, nameCol = None, attribute = None, model = None, sep = None, remoteAttribute = None, localAttr = None, fields = None, typeMatch = None, fixedValue = None):
        match = Match(
                nameCol = nameCol,
                attribute = attribute,
                model = model,
                sep = sep,
                remoteAttribute = remoteAttribute,
                localAttr = localAttr,
                fields = fields,
                typeMatch = typeMatch,
                fixedValue = fixedValue)
        print(f"Agregando match de tipo {typeMatch} y attr {attribute}")
        self.matches.append(match)

    def generateItems(self, records, items={}, bulk_save = False):
        '''
        Records son los récords del Excel
        Items son los items de otros elementos ya guardados
        que se necesitan para guardar al item nuevo
        '''
        objs = []
        errores = []
        for i, record in enumerate(records):
            try:
                modelCopy = deepcopy(self.model)
                obj = modelCopy()
                match: Match
                asignacion = {}
                for match in self.matches:
                    print("---------")
                    print(f"Tipo de match: {match.typeMatch}. Attr, {match.attribute}, remoteAttribute: {match.remoteAttribute}, localAttr: {match.localAttr}")
                    print(f"Assert: {match.attribute} en {[x['name'] for x in self.itemsModel]}")
                    print(f"Récord: {record}")
                    try:
                        assert(match.attribute in [x['name'] for x in self.itemsModel])
                    except:
                        assert(match.attribute + "_id" in [x['name'] for x in self.itemsModel])
                    value = None
                    # La variable forAssignement se da para que el valor que se compute
                    # se asignado directamente. De lo contrario, se hará con .add()
                    # por ser de una relación ManyToManyField
                    forAssignement = True
                    if match.typeMatch == "array_hstore":
                        # Acá la idea es añadir los fields al array
                        # de atributos del objeto.atributo
                        condNone = eval(f"obj.{match.attribute} is None")
                        if condNone:
                            exec(f"obj.{match.attribute} = []")
                        # Hay que modificar fields para que no tome literalmente el valor
                        # de {{value}}
                        if match.fields != None:
                            print(f"A hacer for de {match.fields.keys()}")
                            for key in match.fields.keys():
                                print(f"Una key de este for es: {key}")
                                field = match.fields[key]
                                if type(field) == str:
                                    if "{{column:" in field and "}}" in field:
                                        column = field.replace("{{column:", "").replace("}}", "")
                                        # match.fields[key] = record[column]
                                        asignacion[key] = record[column]
                                    else:
                                        asignacion[key] = field
                                else:
                                    asignacion[key] = None
                        txtExec = f'obj.{match.attribute}.append({asignacion})'
                        exec(txtExec)
                    elif match.typeMatch == "fixed":
                        # Acá hay que asignar directamente una referencia o valor
                        # TODO: El valor no pasarlo como texto!
                        txtExec = f'obj.{match.attribute} = match.fixedValue'
                        print("ALERTA: " + txtExec)
                        exec(txtExec)
                        print(f"Se debió asignar entonces a obj.{match.attribute} el valor: {match.fixedValue}")
                        confirma = eval(f'obj.{match.attribute}')
                        print(f"Confirmación: {confirma}")
                    elif match.typeMatch == 'foreign':
                        forAssignement = False
                        modelForQuery = match.model
                        # Ahora a buscar el item remoto

                        txtQuery = '''modelForQuery.objects.get({0} = "{1}")'''.format(
                                match.remoteAttribute,
                                record[match.nameCol]
                                )
                        modelForeign = eval(txtQuery)
                        # Ahora se puede relacionar
                        txtExec = "obj.{0} = modelForeign".format(
                                match.attribute,
                                )
                        exec(txtExec)
                    elif match.typeMatch == 'manytomany':
                        # Necesitamos guardar para que pueda existir la relación
                        obj.save()
                        # Primer caso especial, esto es de ManyToManyField
                        # Values for search
                        values = record[match.nameCol].split(match.sep)
                        modelForQuery = match.model
                        # manyModel = None
                        for value in values:
                            try:
                                # Primero lo buscamos para que quede en manyModel
                                txtQuery = '''modelForQuery.objects.filter({0} = "{1}").first()'''.format(
                                            match.remoteAttribute,
                                            value
                                        )
                                manyModel = eval(txtQuery)
                                # Ahora tenemos que añadirlo
                                txtExec = "obj.{0}.add(manyModel)".format(match.attribute)
                                exec(txtExec)
                            except Exception as er:
                                print("Hay un error con una asignación ManyToManyField: {0}".format(er))
                                pass
                    elif match.typeMatch == "simple":
                        value = record[match.nameCol]
                        # Antes de la ejecución, vamos a confirmar si es un número y necesita un valor
                        # para no pasarle valores que no son!
                        itemModel_ = [x for x in self.itemsModel if x['name'] == match.attribute]
                        itemModel = {}
                        if len(itemModel_) > 0:
                            itemModel = itemModel_[0]
                        # Chequeo de tipos!
                        noGrabar = False
                        if itemModel.get("type") == "IntegerField":
                            try:
                                exec("int(value)")
                            except:
                                noGrabar = True
                        toExec = '''obj.{0} = value'''.format(
                            match.attribute
                            )
                        if forAssignement and not noGrabar:
                            exec(toExec)
                    else:
                        # Esto creo que debe venir de items, enviando modelos desde
                        # arriba
                        value = items[match.model][i]
                    
                if not bulk_save:
                    print("Se supone que estoy guardando un objeto: {0}".format(obj))
                    # Ahora vamos a utilizar las reglas!
                    # La idea es que venga la columna de donde se va a leer la data
                    # y el separador correspondiente.
                    saveWithKw = False
                    if isinstance(self.saveKwargsRule, dict):
                        # column es la columna de donde se obtendrá la información de los campos
                        # nameKwarg es el nombre del argumento que se enviará a save literalmente
                        # Ej: uoms
                        column = self.saveKwargsRule.get('column')
                        nameKwarg = self.saveKwargsRule.get('nameKwarg')
                        sep = self.saveKwargsRule.get('sep')
                        if column and sep and nameKwarg:
                            dataColumn = record.get(column)
                            if dataColumn:
                                saveWithKw = True
                                txtExecSave = f'''obj.save({nameKwarg} = "{dataColumn}", sep = "{sep}")'''
                                exec(txtExecSave)
                    if not saveWithKw:
                        obj.save()
            except Exception as e:
                raise Exception(e)
                errores.append("Error: {0}".format(str(e)))
                obj = None
            objs.append(obj)
        if bulk_save:
            print("Se supone que estoy gurdando todos los objetos: {0}".format(objs))
            self.model.objects.bulk_create(objs)
        return objs, errores

    def __repr__(self):
        return "Rule for {0} with {1} matches. Order {2}".format(
                self.model,
                len(self.matches),
                self.order
                )

class Match():
    '''
    Match para columnas con atributos o con modelos (Foreign key)
    También para relaciones de ManyToManyField de items ya almacenados
    '''
    def __init__(self, attribute, nameCol = None,
                 model=None, sep=None, remoteAttribute = None,
                 localAttr = None, fields = None, typeMatch=None,
                 fixedValue = None):
        self.attribute = attribute
        self.nameCol = nameCol
        self.model = model
        self.sep = sep
        self.remoteAttribute = remoteAttribute
        self.localAttr = localAttr
        self.fields: Union[Dict, None] = fields
        self.typeMatch = typeMatch
        self.fixedValue = fixedValue

    def __repr__(self):
        if self.nameCol:
            return "Match between {0} and column {1}".format(
                    self.attribute, self.nameCol)
        else:
            return "Match between {0} and {1}".format(
                    self.attribute, self.model
                    )


