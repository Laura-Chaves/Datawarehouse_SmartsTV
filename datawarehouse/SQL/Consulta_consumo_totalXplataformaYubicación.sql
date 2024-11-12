SELECT
    p.nombre_plataforma,
    u.ciudad,
    SUM(c.Total_conexiones_exitosas) AS Total_Conexiones_Exitosas,
    SUM(c.Intentos_acceso_fallido) AS Total_Intentos_Fallidos
FROM
    Consumo c
JOIN
    Plataforma p ON c.ID_Plataforma = p.ID_Plataforma
JOIN
    Ubicacion u ON c.ID_Ubicacion = u.ID_Ubicacion
GROUP BY
    p.nombre_plataforma, u.ciudad, c.ID_Plataforma, c.ID_Ubicacion;
