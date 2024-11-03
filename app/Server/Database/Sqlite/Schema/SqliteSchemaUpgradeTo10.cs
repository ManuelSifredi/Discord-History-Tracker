using System.Threading.Tasks;
using DHT.Server.Database.Sqlite.Utils;

namespace DHT.Server.Database.Sqlite.Schema;

sealed class SqliteSchemaUpgradeTo10 : ISchemaUpgrade
{
    async Task ISchemaUpgrade.Run(ISqliteConnection conn, ISchemaUpgradeCallbacks.IProgressReporter reporter)
    {
        await reporter.MainWork("Applying schema changes...", 0, 1);

        // Cambio 1: Se tuvo que hacer esto porque en SQLite no se puede agregar claves foraneas después de creada la tabla (obviamente no es lo ideal)
        await AddForeignKeyToChannels(conn);
        await AddForeignKeyToMessages(conn);
        await AddForeignKeyToMessageEmbeds(conn);
        await AddForeignKeyToMessageReactions(conn);
        await AddForeignKeyToMessageEditTimeStamps(conn);
        await AddForeignKeyToMessageReplied(conn);

        // ------------------------
        // Cambio 2: Agregar usuario con ID -1 para usarse como placeholder para mensajes de Discord.
        // Esto es para usar en mensajes que hacen reply a mensajes que manda Discord, como mensajes de bienvenida.
        // Eso se hace porque DHT actualmente no los está trackeando
        // Queda por probar como funcionan los mensajes que le hicieron reply a un mensaje que está borrado al momento de hacer el tracking
        await conn.ExecuteAsync("""
                                    INSERT INTO users (id, name, display_name, avatar_url, discriminator)
                                    VALUES (-1, 'Discord', 'Discord', 'https://support.discord.com/hc/user_images/PRywUXcqg0v5DD6s7C3LyQ.jpeg', '0')
                                """);
    }

    private async Task AddForeignKeyToChannels(ISqliteConnection conn)
    {
        // Paso 1: Crear una nueva tabla con claves foráneas
        await conn.ExecuteAsync("""
                                CREATE TABLE channels_temp (
                                    id        INTEGER PRIMARY KEY NOT NULL,
                                    server    INTEGER NOT NULL,
                                    name      TEXT NOT NULL,
                                    parent_id INTEGER,
                                    position  INTEGER,
                                    topic     TEXT,
                                    nsfw      INTEGER,
                                    FOREIGN KEY (server) REFERENCES servers (id) ON UPDATE CASCADE ON DELETE CASCADE
                                )
                                """);

        // Paso 2: Copiar los datos de la tabla original a la nueva
        await conn.ExecuteAsync("""
                                    INSERT INTO channels_temp (id, server, name, parent_id, position, topic, nsfw)
                                    SELECT id, server, name, parent_id, position, topic, nsfw FROM channels
                                """);

        // Paso 3: Eliminar la tabla original
        await conn.ExecuteAsync("DROP TABLE channels");

        // Paso 4: Renombrar la nueva tabla
        await conn.ExecuteAsync("ALTER TABLE channels_temp RENAME TO channels");
    }

    private async Task AddForeignKeyToMessages(ISqliteConnection conn)
    {
        // Paso 1: Crear una nueva tabla con claves foráneas
        await conn.ExecuteAsync("""
                                CREATE TABLE messages_temp (
                                    message_id INTEGER PRIMARY KEY NOT NULL,
                                    sender_id  INTEGER NOT NULL,
                                    channel_id INTEGER NOT NULL,
                                    text       TEXT NOT NULL,
                                    timestamp  INTEGER NOT NULL,
                                    FOREIGN KEY (sender_id) REFERENCES users (id) ON UPDATE CASCADE ON DELETE CASCADE,
                                    FOREIGN KEY (channel_id) REFERENCES channels (id) ON UPDATE CASCADE ON DELETE CASCADE
                                )
                                """);

        // Paso 2: Copiar los datos de la tabla original a la nueva
        await conn.ExecuteAsync("""
                                    INSERT INTO messages_temp (message_id, sender_id, channel_id, text, timestamp)
                                    SELECT message_id, sender_id, channel_id, text, timestamp FROM messages
                                """);

        // Paso 3: Eliminar la tabla original
        await conn.ExecuteAsync("DROP TABLE messages");

        // Paso 4: Renombrar la nueva tabla
        await conn.ExecuteAsync("ALTER TABLE messages_temp RENAME TO messages");
    }

    private async Task AddForeignKeyToMessageEmbeds(ISqliteConnection conn)
    {
        // Paso 1: Crear una nueva tabla con claves foráneas
        await conn.ExecuteAsync("""
		                        CREATE TABLE message_embeds_temp (
		                        	message_id INTEGER NOT NULL,
		                        	json       TEXT NOT NULL,
		                        	FOREIGN KEY (message_id) REFERENCES messages (message_id) ON UPDATE CASCADE ON DELETE CASCADE
		                        )
		                        """);

        // Paso 2: Copiar los datos de la tabla original a la nueva
        await conn.ExecuteAsync("""
                                    INSERT INTO message_embeds_temp (message_id, json)
                                    SELECT message_id, json FROM message_embeds
                                """);

        // Paso 3: Eliminar la tabla original
        await conn.ExecuteAsync("DROP TABLE message_embeds");

        // Paso 4: Renombrar la nueva tabla
        await conn.ExecuteAsync("ALTER TABLE message_embeds_temp RENAME TO message_embeds");
    }

    private async Task AddForeignKeyToMessageReactions(ISqliteConnection conn)
    {
        // Paso 1: Crear una nueva tabla con claves foráneas
        await conn.ExecuteAsync("""
		                        CREATE TABLE message_reactions_temp (
		                        	message_id  INTEGER NOT NULL,
		                        	emoji_id    INTEGER,
		                        	emoji_name  TEXT,
		                        	emoji_flags INTEGER NOT NULL,
		                        	count       INTEGER NOT NULL,
		                        	FOREIGN KEY (message_id) REFERENCES messages (message_id) ON UPDATE CASCADE ON DELETE CASCADE
		                        )
		                        """);

        // Paso 2: Copiar los datos de la tabla original a la nueva
        await conn.ExecuteAsync("""
                                    INSERT INTO message_reactions_temp (message_id, emoji_id, emoji_name, emoji_flags, count)
                                    SELECT message_id, emoji_id, emoji_name, emoji_flags, count FROM message_reactions
                                """);

        // Paso 3: Eliminar la tabla original
        await conn.ExecuteAsync("DROP TABLE message_reactions");

        // Paso 4: Renombrar la nueva tabla
        await conn.ExecuteAsync("ALTER TABLE message_reactions_temp RENAME TO message_reactions");
    }

    private async Task AddForeignKeyToMessageEditTimeStamps(ISqliteConnection conn)
    {
        // Paso 1: Crear una nueva tabla con claves foráneas
        await conn.ExecuteAsync("""
		                        CREATE TABLE message_edit_timestamps_temp (
		                        	message_id     INTEGER PRIMARY KEY NOT NULL,
		                        	edit_timestamp INTEGER NOT NULL,
		                        	FOREIGN KEY (message_id) REFERENCES messages (message_id) ON UPDATE CASCADE ON DELETE CASCADE
		                        )
		                        """);

        // Paso 2: Copiar los datos de la tabla original a la nueva
        await conn.ExecuteAsync("""
                                    INSERT INTO message_edit_timestamps_temp (message_id, edit_timestamp)
                                    SELECT message_id, edit_timestamp FROM message_edit_timestamps;
                                """);

        // Paso 3: Eliminar la tabla original
        await conn.ExecuteAsync("DROP TABLE message_edit_timestamps");

        // Paso 4: Renombrar la nueva tabla
        await conn.ExecuteAsync("ALTER TABLE message_edit_timestamps_temp RENAME TO message_edit_timestamps");
    }

    private async Task AddForeignKeyToMessageReplied(ISqliteConnection conn)
    {
        // Paso 1: Crear una nueva tabla con claves foráneas
        await conn.ExecuteAsync("""
		                        CREATE TABLE message_replied_to_temp (
		                        	message_id    INTEGER PRIMARY KEY NOT NULL,
		                        	replied_to_id INTEGER NOT NULL,
		                        	FOREIGN KEY (message_id) REFERENCES messages (message_id) ON UPDATE CASCADE ON DELETE CASCADE,
		                        	FOREIGN KEY (replied_to_id) REFERENCES messages (message_id) ON UPDATE CASCADE ON DELETE CASCADE
		                        )
		                        """);

        // Paso 2: Copiar los datos de la tabla original a la nueva
        await conn.ExecuteAsync("""
                                    INSERT INTO message_replied_to_temp (message_id, replied_to_id)
                                    SELECT message_id, replied_to_id FROM message_replied_to;
                                """);

        // Paso 3: Eliminar la tabla original
        await conn.ExecuteAsync("DROP TABLE message_replied_to");

        // Paso 4: Renombrar la nueva tabla
        await conn.ExecuteAsync("ALTER TABLE message_replied_to_temp RENAME TO message_replied_to");
    }
}