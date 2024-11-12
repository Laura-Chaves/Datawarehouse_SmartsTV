SELECT
    t.Trimestre,
    SUM(c.Total_conexiones_exitosas) AS Total_Conexiones_Exitosas,
    SUM(c.Intentos_acceso_fallido) AS Total_Intentos_Fallidos
FROM
    Consumo c
JOIN
    Tiempo t ON c.ID_Tiempo = t.ID_Tiempo
GROUP BY
    t.Trimestre
ORDER BY
    t.Trimestre ASC;
