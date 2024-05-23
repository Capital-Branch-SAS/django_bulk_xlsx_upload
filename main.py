from django.db import models
from django.db.models.fields.related import ForeignKey
import pyexcel
from typing import Union
import datetime
from copy import deepcopy

class DjangoBulkXLSXUpload():
    '''
    Main class for bulk upload of Django models from a
    Microsoft Excel XLSX file
    '''

    def __init__(self, rules = None):
        self.file = None
        self.sheet = None
        self.records = []
        self.bulk_save = False
        # Array por modelos
        self.saved_models = {}
        self.rules: list[UploadRule] = []
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
            if i > column_by_row:
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
        Estructura ejemplo de las reglas:
            {
                Usuario: {"name": "Nombre",
                           "lastname": "Apellido",
                           "permisos": Permisos,
                           "roles": (Rol, "Roles", "name", "-")
                          },
                Permisos: {"lectura": "Lectura",
                           "escritura": "Escritura"}
            }
        '''
        for model in data.keys():
            assert(isinstance(model, models.base.ModelBase))
            values = data[model]
            rule = UploadRule(model)
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
                column: Union[models.base.ModelBase, str, tuple] = values[keyModel]
                isColumn = isinstance(column, str) and column != ""
                isModel = isinstance(column, models.base.ModelBase)
                isTuple = isinstance(column, tuple) and column != ""
                if not (isColumn or isModel or isTuple):
                    raise Exception(
                        "Value of {0} isn't string or a valid Django Model".format(column))
                if isinstance(column, tuple):
                    lenTupla = len(column)
                    if lenTupla not in [4, 5]:
                        raise Exception("Tuple {0} isn't good defined enough".format(column))
                    if lenTupla == 4:
                        rule.addMatch(
                                keyModel,
                                attribute= column[1],
                                model = column[0],
                                sep = column[3],
                                remoteAttribute = column[2],
                                )
                    else:
                        rule.addMatch(
                                keyModel,
                                attribute = column[1],
                                model = column[0],
                                remoteAttribute = column[2],
                                localAttr= column[3]
                                )
                else:
                    rule.addMatch(
                            keyModel,
                            attribute = column if isColumn else None,
                            model = column if isModel else None)
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
    def __init__(self, model):
        self.model: models.Model = model
        self.matches: list[Match] = []
        self.order: int = 0
        self._ordered = False
        # objs será una lista de mapas con la estructura:
        # {index: int, obj: obj | None}
        self.objs = []
        # itemsModel contendrá la lista de los items
        # del modelo junto con sus tipos
        self.itemsModel = []
        for field in model._meta.fields:
            self.itemsModel.append({
                "name": field.attname,
                "type": field.get_internal_type()
                })

    def addMatch(self, nameCol, attribute = None, model = None, sep = None, remoteAttribute = None, localAttr = None):
        match = Match(nameCol, attribute, model, sep, remoteAttribute, localAttr)
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
                for match in self.matches:
                    value = None
                    # La variable forAssignement se da para que el valor que se compute
                    # se asignado directamente. De lo contrario, se hará con .add()
                    # por ser de una relación ManyToManyField
                    forAssignement = True
                    if match.remoteAttribute != None:
                        forAssignement = False
                        # Van a haber dos casos, el de ManyToManyField y el de ForeignKey!
                        if not match.sep:
                            # En este caso, es ForeignKey
                            modelForQuery = match.model
                            # Ahora a buscar el item remoto

                            txtQuery = '''modelForQuery.objects.get({0} = "{1}")'''.format(
                                    match.remoteAttribute,
                                    record[match.nameCol]
                                    )
                            modelForeign = eval(txtQuery)
                            # Ahora se puede relacionar
                            txtExec = "obj.{0} = modelForeign".format(
                                    match.localAttr,
                                    )
                            exec(txtExec)
                        else:
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
                    elif match.nameCol:
                        assert(match.attribute in [x['name'] for x in self.itemsModel])
                        value = record[match.nameCol]
                    else:
                        # Esto creo que debe venir de items, enviando modelos desde
                        # arriba
                        value = items[match.model][i]
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
                if not bulk_save:
                    print("Se supone que estoy guardando un objeto: {0}".format(obj))
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
                 localAttr = None):
        self.attribute = attribute
        self.nameCol = nameCol
        self.model = model
        self.sep = sep
        self.remoteAttribute = remoteAttribute
        self.localAttr = localAttr
        if (not nameCol) and (not model):
            raise Exception("Neither nameCol or model are defined")
        if nameCol and model and not remoteAttribute:
            raise Exception("Both nameCol and model are defined")

    def __repr__(self):
        if self.nameCol:
            return "Match between {0} and column {1}".format(
                    self.attribute, self.nameCol)
        else:
            return "Match between {0} and {1}".format(
                    self.attribute, self.model
                    )


