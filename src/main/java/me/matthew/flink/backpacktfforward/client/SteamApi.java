package me.matthew.flink.backpacktfforward.client;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import me.matthew.flink.backpacktfforward.config.SteamApiConfiguration;
import me.matthew.flink.backpacktfforward.model.InventoryItem;
import me.matthew.flink.backpacktfforward.model.SteamInventoryResponse;
import org.apache.flink.metrics.Counter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HTTP client for querying Steam Web API to retrieve user inventory data.
 * Follows existing patterns for HTTP communication and error handling.
 * Includes retry logic using Failsafe for resilient API calls.
 * Tracks metrics for API call success/failure rates.
 */
@Slf4j
public class SteamApi {
    
    private static final String STEAM_API_BASE_URL = "https://api.steampowered.com/IEconItems_440/GetPlayerItems/v0001/";
    
    // Rate limiting: configurable delay between requests (default 10 seconds for 6 requests per minute)
    private static final Duration RATE_LIMIT_DELAY = Duration.ofSeconds(SteamApiConfiguration.getSteamApiRateLimitSeconds());
    
    private final String apiKey;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final RetryPolicy<SteamInventoryResponse> retryPolicy;
    
    // Metrics for tracking API performance
    private final Counter steamApiCallsSuccess;
    private final Counter steamApiCallsFailed;
    
    // Rate limiting state - shared across all instances to prevent multiple clients from overwhelming the API
    private static final ReentrantLock rateLimitLock = new ReentrantLock();
    private static volatile Instant lastApiCall = Instant.EPOCH;
    
    /**
     * Creates a new SteamApi using API key from environment variables.
     * 
     * @throws IllegalStateException if the API key is not configured
     */
    public SteamApi() {
        this(SteamApiConfiguration.getSteamApiKey(), null, null);
    }
    public SteamApi(String apiKey) {
        this(apiKey, null, null);
    }
    public SteamApi(String apiKey, Counter steamApiCallsSuccess, Counter steamApiCallsFailed) {
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Steam API key cannot be null or empty");
        }
        
        this.apiKey = apiKey;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(SteamApiConfiguration.getSteamApiTimeoutSeconds()))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.objectMapper = new ObjectMapper();
        this.retryPolicy = createRetryPolicy();
        this.steamApiCallsSuccess = steamApiCallsSuccess;
        this.steamApiCallsFailed = steamApiCallsFailed;
    }
    
    /**
     * Creates a retry policy for Steam API calls with limited retries and exponential backoff.
     * Logs warnings when retry attempts exceed 10.
     * 
     * @return RetryPolicy configured for Steam API operations
     */
    private RetryPolicy<SteamInventoryResponse> createRetryPolicy() {
        return RetryPolicy.<SteamInventoryResponse>builder()
                .handle(IOException.class)
                .handle(HttpTimeoutException.class)
                .handle(JsonParseException.class) // Handle JSON parsing failures
                .handleIf(this::isRetryableHttpError)
                .withDelay(Duration.ofSeconds(2))
                .withMaxRetries(-1)
                .withBackoff(Duration.ofSeconds(2), Duration.ofMinutes(5))
                .onRetry(e -> {
                    if (e.getAttemptCount() > 10) {
                        log.warn("Steam API retry attempt {} (EXCESSIVE): {}. This may indicate persistent API issues.",
                                e.getAttemptCount(),
                                e.getLastException().getMessage());
                    } else {
                        log.debug("Steam API retry (attempt {}): {}",
                                e.getAttemptCount(),
                                e.getLastException().getMessage());
                    }
                })
                .onFailure(e -> {
                    log.error("Steam API failed after {} attempts. Last error: {}", 
                            e.getAttemptCount(), 
                            e.getException().getMessage());
                })
                .build();
    }
    
    private boolean isRetryableHttpError(Throwable throwable) {
        // Handle JSON parsing exceptions - these are often due to corrupted responses
        if (throwable instanceof JsonParseException) {
            log.warn("JSON parsing failed, likely due to corrupted Steam API response: {}", throwable.getMessage());
            return true; // Retry JSON parsing failures
        }
        
        if (throwable instanceof IOException) {
            String message = throwable.getMessage();
            if (message != null) {
                // Don't retry authentication failures (401, 403) - these indicate API key issues
                if (message.contains("status 401") || message.contains("status 403")) {
                    return false;
                }
                // Retry on rate limiting (429), server errors (5xx), timeouts, and parsing failures
                return message.contains("status 429") || 
                       message.contains("status 503") ||  // Service unavailable - common with Steam API
                       message.contains("status 5") ||
                       message.contains("timeout") ||
                       message.contains("Failed to parse Steam API response"); // Our custom parsing error
            }
        }
        return false;
    }
    
    private void enforceRateLimit() throws InterruptedException {
        rateLimitLock.lock();
        try {
            Instant now = Instant.now();
            Duration timeSinceLastCall = Duration.between(lastApiCall, now);
            
            if (timeSinceLastCall.compareTo(RATE_LIMIT_DELAY) < 0) {
                Duration waitTime = RATE_LIMIT_DELAY.minus(timeSinceLastCall);
                log.debug("Rate limiting: waiting {} ms before next Steam API call", waitTime.toMillis());
                Thread.sleep(waitTime.toMillis());
            }
            
            lastApiCall = Instant.now();
        } finally {
            rateLimitLock.unlock();
        }
    }
    
    /**
     * Retrieves a Steam user's inventory items for TF2.
     * Uses retry logic to handle transient failures and Steam API rate limits.
     * 
     * @param steamId The Steam ID of the user whose inventory to retrieve
     * @return SteamInventoryResponse containing the inventory data
     * @throws IOException if the HTTP request fails after all retries
     * @throws InterruptedException if the request is interrupted
     * @throws URISyntaxException if the URL construction fails
     */
    public SteamInventoryResponse getPlayerItems(String steamId) 
            throws IOException, InterruptedException, URISyntaxException {
        
        log.debug("Fetching Steam inventory for steamId={}", steamId);
        
        return Failsafe.with(retryPolicy).get(() -> performInventoryApiCall(steamId));
    }
    
    /**
     * Performs the actual Steam API call without retry logic.
     * Enforces rate limiting before making the call.
     * 
     * @param steamId The Steam ID of the user
     * @return SteamInventoryResponse containing the inventory data
     * @throws IOException if the HTTP request fails or response cannot be parsed
     * @throws InterruptedException if the request is interrupted
     * @throws URISyntaxException if the URL construction fails
     */
    private SteamInventoryResponse performInventoryApiCall(String steamId) 
            throws IOException, InterruptedException, URISyntaxException {
        
        // Enforce rate limiting before making the call
        enforceRateLimit();
        
        // Build the request URL with required parameters (URL-encode parameters for safety)
        String encodedApiKey = URLEncoder.encode(apiKey, StandardCharsets.UTF_8);
        String encodedSteamId = URLEncoder.encode(steamId, StandardCharsets.UTF_8);
        String url = STEAM_API_BASE_URL + String.format("?key=%s&steamid=%s&format=json", encodedApiKey, encodedSteamId);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .timeout(Duration.ofSeconds(SteamApiConfiguration.getSteamApiTimeoutSeconds()))
                .header("User-Agent", "TF2-Custom-Pricer/1.0")
                .header("Accept", "application/json")
                .header("Accept-Encoding", "identity") // Only accept uncompressed responses to avoid corruption
                .header("Accept-Charset", "UTF-8")
                .GET()
                .build();
        
        log.debug("Making Steam API request for steamId: {} to URL: {}", steamId, url);
        
        HttpResponse<String> response = httpClient.send(request, 
                HttpResponse.BodyHandlers.ofString());
        
        log.debug("Steam API response status: {}, headers: {}", response.statusCode(), response.headers().map());
        
        // Handle different HTTP status codes
        if (response.statusCode() == 503) {
            if (steamApiCallsFailed != null) steamApiCallsFailed.inc();
            log.warn("Steam API returned 503 Service Unavailable. This often indicates API overload or maintenance. Response: {}", response.body());
            throw new IOException("Steam API service unavailable (status 503) - will retry with exponential backoff");
        } else if (response.statusCode() == 429) {
            if (steamApiCallsFailed != null) steamApiCallsFailed.inc();
            throw new IOException("Rate limited by Steam API (status 429) - will retry with exponential backoff");
        } else if (response.statusCode() >= 500) {
            if (steamApiCallsFailed != null) steamApiCallsFailed.inc();
            throw new IOException(String.format(
                    "Steam API server error (status %d): %s", 
                    response.statusCode(), response.body()));
        } else if (response.statusCode() == 401 || response.statusCode() == 403) {
            // Don't retry authentication failures - log and fail fast
            if (steamApiCallsFailed != null) steamApiCallsFailed.inc();
            log.error("Steam API authentication failed (status {}). Check API key validity and permissions. Response: {}", 
                    response.statusCode(), response.body());
            throw new IOException("Steam API authentication failed - check API key (status " + response.statusCode() + ")");
        } else if (response.statusCode() != 200) {
            if (steamApiCallsFailed != null) steamApiCallsFailed.inc();
            throw new IOException(String.format(
                    "Steam API request failed with status %d: %s", 
                    response.statusCode(), response.body()));
        }
        
        try {
            String responseBody = response.body();
            
            // Validate and clean the response body before parsing
            String cleanedBody = validateAndCleanResponse(responseBody);
            
            SteamInventoryResponse inventoryResponse = objectMapper.readValue(
                    cleanedBody, SteamInventoryResponse.class);
            
            // Check if the API call was successful
            if (inventoryResponse.getResult() == null || inventoryResponse.getResult().getStatus() != 1) {
                if (steamApiCallsFailed != null) steamApiCallsFailed.inc();
                throw new IOException("Steam API returned unsuccessful status: " + 
                        (inventoryResponse.getResult() != null ? inventoryResponse.getResult().getStatus() : "null"));
            }
            
            int itemCount = inventoryResponse.getResult().getItems() != null ? 
                    inventoryResponse.getResult().getItems().size() : 0;
            log.debug("Successfully parsed Steam inventory with {} items", itemCount);
            
            // Record successful API call
            if (steamApiCallsSuccess != null) steamApiCallsSuccess.inc();
            
            return inventoryResponse;
        } catch (Exception e) {
            if (steamApiCallsFailed != null) steamApiCallsFailed.inc();
            log.error("Failed to parse Steam API response: {}", sanitizeForLogging(response.body()), e);
            throw new IOException("Failed to parse Steam API response", e);
        }
    }
    
    /**
     * Validates and cleans the API response body to handle corrupted data.
     * Removes control characters and validates JSON structure.
     * 
     * @param responseBody The raw response body from Steam API
     * @return Cleaned response body safe for JSON parsing
     * @throws IOException if the response is too corrupted to recover
     */
    private String validateAndCleanResponse(String responseBody) throws IOException {
        if (responseBody == null || responseBody.trim().isEmpty()) {
            throw new IOException("Steam API returned empty response");
        }
        
        // Check for obvious binary corruption (control characters at the start)
        if (responseBody.length() > 0 && responseBody.charAt(0) < 32 && responseBody.charAt(0) != '\t' && 
            responseBody.charAt(0) != '\n' && responseBody.charAt(0) != '\r') {
            log.warn("Steam API response starts with control character (code {}), attempting to clean", 
                    (int) responseBody.charAt(0));
        }
        
        // Remove all control characters except valid JSON whitespace
        String cleaned = responseBody.replaceAll("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F]", "");
        
        // Trim whitespace
        cleaned = cleaned.trim();
        
        // Basic JSON structure validation
        if (!cleaned.startsWith("{") || !cleaned.endsWith("}")) {
            // Try to find JSON boundaries in case of prefix/suffix corruption
            int jsonStart = cleaned.indexOf('{');
            int jsonEnd = cleaned.lastIndexOf('}');
            
            if (jsonStart >= 0 && jsonEnd > jsonStart) {
                log.warn("Steam API response has corrupted boundaries, extracting JSON from position {} to {}", 
                        jsonStart, jsonEnd);
                cleaned = cleaned.substring(jsonStart, jsonEnd + 1);
            } else {
                throw new IOException("Steam API response does not contain valid JSON structure. " +
                        "Response preview: " + sanitizeForLogging(responseBody.substring(0, Math.min(200, responseBody.length()))));
            }
        }
        
        // Additional validation - check for reasonable JSON size
        if (cleaned.length() < 10) {
            throw new IOException("Steam API response too short to be valid JSON: " + sanitizeForLogging(cleaned));
        }
        
        // Log if significant cleaning was performed
        if (!cleaned.equals(responseBody)) {
            log.warn("Steam API response required cleaning. Original length: {}, cleaned length: {}", 
                    responseBody.length(), cleaned.length());
        }
        
        return cleaned;
    }
    
    /**
     * Sanitizes response content for safe logging by removing control characters.
     * 
     * @param content The content to sanitize
     * @return Sanitized content safe for logging
     */
    private String sanitizeForLogging(String content) {
        if (content == null) {
            return "null";
        }
        
        // Replace control characters with their Unicode escape sequences for visibility
        return content.replaceAll("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F]", "�")
                     .substring(0, Math.min(500, content.length())); // Limit length for logs
    }

    /**
     * Finds all items in the inventory that match the specified defindex and quality.
     * Uses exact matching for both defindex and quality fields.
     * 
     * @param inventory The Steam inventory response to search
     * @param targetDefindex The item definition index to match
     * @param targetQuality The item quality to match
     * @return List of matching items (empty list if no matches found)
     */
    public List<InventoryItem> findMatchingItems(SteamInventoryResponse inventory, 
            int targetDefindex, int targetQuality) {
        
        List<InventoryItem> matchingItems = new ArrayList<>();
        
        if (inventory == null || inventory.getResult() == null || inventory.getResult().getItems() == null) {
            log.debug("No inventory data to search for defindex={}, quality={}", targetDefindex, targetQuality);
            return matchingItems;
        }
        
        log.debug("Searching inventory with {} items for defindex={}, quality={}", 
                inventory.getResult().getItems().size(), targetDefindex, targetQuality);
        
        for (InventoryItem item : inventory.getResult().getItems()) {
            if (item.getDefindex() == targetDefindex && item.getQuality() == targetQuality) {
                matchingItems.add(item);
            }
        }
        
        log.debug("Found {} matching items for defindex={}, quality={}", 
                matchingItems.size(), targetDefindex, targetQuality);
        
        return matchingItems;
    }
}