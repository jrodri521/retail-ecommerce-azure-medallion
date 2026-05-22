	from pyspark.sql import SparkSession
	
	spark = SparkSession.builder.getOrCreate()
	
	storage_account_name = "stretaildevjr"
	container_name = "bronze"
	sas_token = "?sp=rcwdl&st=2026-05-18T22:29:00Z&se=2026-05-19T06:44:00Z&spr=https&sv=2025-11-05&sr=c&sig=%2B%2B%2FIelf1t94f2y4svVC%2B9ah15p9bSDYUeNvBzhobLIQ%3D"  # Reemplaza por tu SAS real o usa un secret
	
	if sas_token.startswith("<") or not sas_token.strip():
	    raise ValueError(
	        "Configura un SAS token válido antes de ejecutar este notebook. "
	        "Sugerencia: usa dbutils.secrets.get(...) y elimina el prefijo '?' si existe."
	    )
	
	sas_token = sas_token.lstrip("?")
	
	spark.conf.set(
	    f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net",
	    sas_token,
	)
	
	bronze_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
	
	SEEDS = {
	    "articulos": [11, 12, 13, 14, 15, 16],
	    "proveedores": [21, 22, 23, 24],
	    "tiendas": [31, 32, 33, 34, 35],
	    "miembros": [41, 42, 43, 44, 45],
	    "ventas": [51, 52, 53, 54, 55, 56, 57, 58],
	    "stock": [61, 62, 63, 64, 65, 66],
	    "devoluciones": [71, 72, 73, 74, 75, 76, 77],
	}
	
	PARTITIONS = {
	    "MSTR_ARTICULOS": 4,
	    "MSTR_PROVEEDORES": 2,
	    "MSTR_TIENDAS": 2,
	    "CRM_MIEMBROS": 8,
	    "TRANS_VENTAS": 16,
	    "INV_STOCK_DIARIO": 12,
	    "POST_DEVOLUCIONES": 6,
	}
	
	def write_parquet(df, dataset_name, partition_cols=None):
	    writer = df.repartition(PARTITIONS[dataset_name]).write.mode("overwrite")
	    if partition_cols:
	        writer = writer.partitionBy(*partition_cols)
	    writer.parquet(f"{bronze_path}{dataset_name}")
	
	# 1. MSTR_ARTICULOS (5.000 registros)
	articulos = spark.range(1, 5001).selectExpr(
	    "id as art_id",
	    "concat('COD', lpad(cast(id as string), 6, '0')) as cod_barra",
	    "concat('Producto_', id) as desc_art",
	    f"cast(rand({SEEDS['articulos'][0]}) * 10 as int) as id_categ_n1",
	    f"cast(rand({SEEDS['articulos'][1]}) * 50 as int) as id_categ_n2",
	    f"cast(rand({SEEDS['articulos'][2]}) * 200 as int) as id_categ_n3",
	    f"cast(rand({SEEDS['articulos'][3]}) * 800 as int) as id_proveedor",
	    "current_date() as fec_alta",
	    f"round(rand({SEEDS['articulos'][4]}) * 100000, 2) as precio_lista",
	    f"round(rand({SEEDS['articulos'][5]}) * 5, 2) as peso_kg",
	    "'UN' as unid_medida",
	    "true as activo",
	)
	write_parquet(articulos, "MSTR_ARTICULOS")
	
	# 2. MSTR_PROVEEDORES (800 registros)
	proveedores = spark.range(1, 801).selectExpr(
	    "id as id_proveedor",
	    "concat('Proveedor_', id) as razon_social",
	    f"concat('Pais_', cast(rand({SEEDS['proveedores'][0]}) * 10 as int)) as pais_origen",
	    f"cast(rand({SEEDS['proveedores'][1]}) * 30 as int) as tiempo_repo_dias",
	    f"round(rand({SEEDS['proveedores'][2]}) * 5, 1) as calificacion_calidad",
	    "true as activo",
	)
	write_parquet(proveedores, "MSTR_PROVEEDORES")
	
	# 3. MSTR_TIENDAS (150 registros)
	tiendas = spark.range(1, 151).selectExpr(
	    "id as id_tienda",
	    "concat('Tienda_', id) as nom_tienda",
	    f"concat('Tipo_', cast(rand({SEEDS['tiendas'][0]}) * 3 as int)) as tipo_tienda",
	    f"cast(rand({SEEDS['tiendas'][1]}) * 100 as int) as id_ciudad",
	    f"cast(rand({SEEDS['tiendas'][2]}) * 5 as int) as id_pais",
	    f"cast(rand({SEEDS['tiendas'][3]}) * 2000 as int) as metros_cuadrados",
	    "true as activo",
	    f"date_add(current_date(), -cast(rand({SEEDS['tiendas'][4]}) * 5000 as int)) as fec_apertura",
	)
	write_parquet(tiendas, "MSTR_TIENDAS")
	
	# 4. CRM_MIEMBROS (50.000 registros)
	miembros = spark.range(1, 50001).selectExpr(
	    "id as id_miembro",
	    f"date_add(current_date(), -cast(rand({SEEDS['miembros'][0]}) * 2000 as int)) as fec_registro",
	    f"cast(rand({SEEDS['miembros'][1]}) * 100 as int) as id_ciudad",
	    f"CASE WHEN rand({SEEDS['miembros'][2]}) < 0.5 THEN 'M' ELSE 'F' END as genero",
	    f"cast(rand({SEEDS['miembros'][3]}) * 5 as int) as rango_edad",
	    f"concat('Canal_', cast(rand({SEEDS['miembros'][4]}) * 3 as int)) as canal_pref",
	    "true as activo",
	    "date_add(current_date(), -cast(rand(46) * 180 as int)) as fec_ultima_compra",
	)
	write_parquet(miembros, "CRM_MIEMBROS")
	
	# 5. TRANS_VENTAS (1.000.000 registros)
	ventas = spark.range(1, 1000001).selectExpr(
	    "id as id_trans",
	    f"cast(rand({SEEDS['ventas'][0]}) * 50000 as int) + 1 as id_miembro",
	    f"cast(rand({SEEDS['ventas'][1]}) * 150 as int) + 1 as id_tienda",
	    f"cast(rand({SEEDS['ventas'][2]}) * 5000 as int) + 1 as art_id",
	    f"date_add(current_date(), -cast(rand({SEEDS['ventas'][3]}) * 365 as int)) as fec_trans",
	    f"concat(lpad(cast(cast(rand({SEEDS['ventas'][4]}) * 24 as int) as string), 2, '0'), ':00') as hra_trans",
	    f"cast(rand({SEEDS['ventas'][5]}) * 10 + 1 as int) as qty_vendida",
	    f"round(rand({SEEDS['ventas'][6]}) * 100000, 2) as precio_unitario_venta",
	    f"round(rand({SEEDS['ventas'][7]}) * 5000, 2) as descuento_aplicado",
	    "concat('Pago_', cast(rand(59) * 5 as int)) as tipo_pago",
	    "concat('Canal_', cast(rand(60) * 3 as int)) as canal_venta",
	)
	write_parquet(ventas, "TRANS_VENTAS", partition_cols=["fec_trans"])
	
	# 6. INV_STOCK_DIARIO (750.000 registros)
	stock = spark.range(1, 750001).selectExpr(
	    "id as id_snapshot",
	    f"cast(rand({SEEDS['stock'][0]}) * 5000 as int) + 1 as art_id",
	    f"cast(rand({SEEDS['stock'][1]}) * 150 as int) + 1 as id_tienda",
	    f"date_add(current_date(), -cast(rand({SEEDS['stock'][2]}) * 30 as int)) as fec_snapshot",
	    f"cast(rand({SEEDS['stock'][3]}) * 500 as int) as stock_fisico",
	    f"cast(rand({SEEDS['stock'][4]}) * 200 as int) as stock_transito",
	    f"cast(rand({SEEDS['stock'][5]}) * 100 as int) as stock_reservado",
	    "cast(rand(67) * 50 as int) as stock_minimo_config",
	    "cast(rand(68) * 1000 as int) as stock_maximo_config",
	)
	write_parquet(stock, "INV_STOCK_DIARIO", partition_cols=["fec_snapshot"])
	
	# 7. POST_DEVOLUCIONES (50.000 registros)
	devoluciones = spark.range(1, 50001).selectExpr(
	    "id as id_devolucion",
	    f"cast(rand({SEEDS['devoluciones'][0]}) * 1000000 as int) + 1 as id_trans_origen",
	    f"cast(rand({SEEDS['devoluciones'][1]}) * 5000 as int) + 1 as art_id",
	    f"cast(rand({SEEDS['devoluciones'][2]}) * 150 as int) + 1 as id_tienda",
	    f"date_add(current_date(), -cast(rand({SEEDS['devoluciones'][3]}) * 180 as int)) as fec_devolucion",
	    f"cast(rand({SEEDS['devoluciones'][4]}) * 5 + 1 as int) as qty_devuelta",
	    f"cast(rand({SEEDS['devoluciones'][5]}) * 10 as int) as motivo_cod",
	    f"concat('Canal_', cast(rand({SEEDS['devoluciones'][6]}) * 3 as int)) as canal_devolucion",
	    "concat('Estado_', cast(rand(78) * 2 as int)) as estado_devolucion",
	    "round(rand(79) * 50000, 2) as vr_reembolso",
	)
	write_parquet(devoluciones, "POST_DEVOLUCIONES", partition_cols=["fec_devolucion"])
	
	print("Datos sintéticos corregidos y optimizados para Bronze.")
	print(f"Ruta base: {bronze_path}")
	print("Datasets generados: MSTR_ARTICULOS, MSTR_PROVEEDORES, MSTR_TIENDAS, CRM_MIEMBROS, TRANS_VENTAS, INV_STOCK_DIARIO, POST_DEVOLUCIONES")

